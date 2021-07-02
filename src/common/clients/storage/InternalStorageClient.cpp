/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/clients/storage/InternalStorageClient.h"
#include "common/base/Base.h"

namespace nebula {
namespace storage {

template <typename T>
::nebula::cpp2::ErrorCode getErrorCode(T& tryResp) {
    if (!tryResp.hasValue()) {
        LOG(ERROR) << tryResp.exception().what();
        return nebula::cpp2::ErrorCode::E_RPC_FAILURE;
    }

    auto& stResp = tryResp.value();
    if (!stResp.ok()) {
        switch (stResp.status().code()) {
            case Status::Code::kLeaderChanged:
                return nebula::cpp2::ErrorCode::E_LEADER_CHANGED;
            default:
                LOG(ERROR) << "not impl error transform: code="
                           << static_cast<int32_t>(stResp.status().code());
        }
        return nebula::cpp2::ErrorCode::E_UNKNOWN;
    }

    auto& failedPart = stResp.value().get_result().get_failed_parts();
    for (auto& p : failedPart) {
        return p.code;
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

void InternalStorageClient::chainUpdateEdge(cpp2::UpdateEdgeRequest& reversedRequest,
                                            TermID termOfSrc,
                                            folly::Promise<::nebula::cpp2::ErrorCode>&& p,
                                            folly::EventBase* evb) {
    auto optLeader = getLeader(reversedRequest.get_space_id(), reversedRequest.get_part_id());
    if (!optLeader.ok()) {
        p.setValue(::nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND);
        return;
    }
    HostAddr& leader = optLeader.value();
    leader.port += kInternalPortOffset;

    cpp2::ChainUpdateEdgeRequest chainReq;
    chainReq.set_update_edge_request(reversedRequest);
    chainReq.set_term(termOfSrc);
    auto resp = getResponse(
        evb,
        std::make_pair(leader, chainReq),
        [](cpp2::InternalStorageServiceAsyncClient* client, const cpp2::ChainUpdateEdgeRequest& r) {
            return client->future_chainUpdateEdge(r);
        });

    std::move(resp).thenTry([=, p = std::move(p)](auto&& t) mutable {
        auto code = getErrorCode(t);
        if (code == ::nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            chainUpdateEdge(reversedRequest, termOfSrc, std::move(p));
        } else {
            p.setValue(code);
        }
        return;
    });
}

void InternalStorageClient::chainAddEdges(cpp2::AddEdgesRequest& directReq,
                                          TermID termId,
                                          folly::Promise<nebula::cpp2::ErrorCode>&& p,
                                          folly::EventBase* evb) {
    auto partId = directReq.get_parts().begin()->first;
    auto optLeader = getLeader(directReq.get_space_id(), partId);
    if (!optLeader.ok()) {
        p.setValue(::nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND);
        return;
    }
    HostAddr& leader = optLeader.value();
    leader.port += kInternalPortOffset;

    cpp2::ChainAddEdgesRequest chainReq = makeChainAddReq(directReq, termId);
    auto resp = getResponse(
        evb,
        std::make_pair(leader, chainReq),
        [](cpp2::InternalStorageServiceAsyncClient* client, const cpp2::ChainAddEdgesRequest& r) {
            return client->future_chainAddEdges(r);
        });

    std::move(resp).thenTry([=, p = std::move(p)](auto&& t) mutable {
        auto code = getErrorCode(t);
        if (code == nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            chainAddEdges(directReq, termId, std::move(p));
        } else {
            p.setValue(code);
        }
        return;
    });
}

cpp2::ChainAddEdgesRequest InternalStorageClient::makeChainAddReq(const cpp2::AddEdgesRequest& req,
                                                                  TermID termId,
                                                                  std::vector<int64_t>* pEdgeVer) {
    cpp2::ChainAddEdgesRequest ret;
    ret.set_space_id(req.get_space_id());
    ret.set_parts(req.get_parts());
    ret.set_prop_names(req.get_prop_names());
    ret.set_if_not_exists(req.get_if_not_exists());
    ret.set_term(termId);
    UNUSED(pEdgeVer);
    // if (pEdgeVer) {
    //     ret.set_edge_ver(*pEdgeVer);
    // }
    return ret;
}

}   // namespace storage
}   // namespace nebula
