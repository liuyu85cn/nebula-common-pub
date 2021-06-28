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
    if (pEdgeVer) {
        ret.set_edge_version(*pEdgeVer);
    }
    return ret;
}

// void InternalStorageClient::forwardAddEdgesImpl(
//     int64_t txnId,
//     GraphSpaceID spaceId,
//     PartitionID partId,
//     std::string&& data,
//     folly::EventBase* evb = nullptr) {
//         VLOG(1) << __func__;
//     }

// StatusOr<HostAddr> InternalStorageClient::getFuzzyLeader(GraphSpaceID spaceId,
//                                                          PartitionID partId) const {
//     return getLeader(spaceId, partId);
// }

// void InternalStorageClient::forwardTransactionImpl(int64_t txnId,
//                                                    GraphSpaceID spaceId,
//                                                    PartitionID partId,
//                                                    std::string&& data,
//                                                    folly::Promise<cpp2::ErrorCode> p,
//                                                    folly::EventBase* evb) {
//     VLOG(1) << "forwardTransactionImpl txnId=" << txnId;
//     auto statusOrLeader = getFuzzyLeader(spaceId, partId);
//     if (!statusOrLeader.ok()) {
//         p.setValue(cpp2::ErrorCode::E_SPACE_NOT_FOUND);
//         return;
//     }
//     HostAddr& dest = statusOrLeader.value();
//     dest.port += kInternalPortOffset;

//     cpp2::InternalTxnRequest interReq;
//     interReq.set_txn_id(txnId);
//     interReq.set_space_id(spaceId);
//     interReq.set_part_id(partId);
//     interReq.set_position(1);
//     (*interReq.data_ref()).resize(2);
//     (*interReq.data_ref()).back().emplace_back(data);
//     getResponse(
//         evb,
//         std::make_pair(dest, interReq),
//         [](cpp2::InternalStorageServiceAsyncClient* client, const cpp2::InternalTxnRequest& r) {
//             return client->future_forwardTransaction(r);
//         })
//         .thenTry([=, p = std::move(p)](auto&& t) mutable {
//             auto code = getErrorCode(t);
//             if (code == cpp2::ErrorCode::E_LEADER_CHANGED) {
//                 std::this_thread::sleep_for(std::chrono::milliseconds(500));
//                 return forwardTransactionImpl(
//                     txnId, spaceId, partId, std::move(data), std::move(p), evb);
//             } else {
//                 p.setValue(code);
//             }
//         });
// }

// folly::SemiFuture<ErrOrVal> InternalStorageClient::getValue(size_t vIdLen,
//                                                             GraphSpaceID spaceId,
//                                                             folly::StringPiece key,
//                                                             folly::EventBase* evb) {
//     auto srcVid = key.subpiece(sizeof(PartitionID), vIdLen);
//     auto stPartId = metaClient_->partId(spaceId, srcVid.str());
//     if (!stPartId.ok()) {
//         return cpp2::ErrorCode::E_SPACE_NOT_FOUND;
//     }

//     auto c = folly::makePromiseContract<ErrOrVal>();
//     getValueImpl(spaceId, stPartId.value(), key.str(), std::move(c.first), evb);
//     return std::move(c.second);
// }

// void InternalStorageClient::getValueImpl(GraphSpaceID spaceId,
//                                          PartitionID partId,
//                                          std::string&& key,
//                                          folly::Promise<ErrOrVal> p,
//                                          folly::EventBase* evb) {
//     std::pair<HostAddr, cpp2::GetValueRequest> req;
//     auto stLeaderHost = getFuzzyLeader(spaceId, partId);
//     if (!stLeaderHost.ok()) {
//         if (stLeaderHost.status().toString().find("partid")) {
//             p.setValue(cpp2::ErrorCode::E_PART_NOT_FOUND);
//         } else {
//             p.setValue(cpp2::ErrorCode::E_SPACE_NOT_FOUND);
//         }
//         return;
//     }
//     req.first = stLeaderHost.value();
//     req.first.port += kInternalPortOffset;

//     req.second.set_space_id(spaceId);
//     req.second.set_part_id(partId);
//     req.second.set_key(key);

//     auto remote = [](cpp2::InternalStorageServiceAsyncClient* client,
//                      const cpp2::GetValueRequest& r) { return client->future_getValue(r); };

//     auto cb = [=, p = std::move(p)](auto&& t) mutable {
//         auto code = getErrorCode(t);
//         if (code == cpp2::ErrorCode::SUCCEEDED) {
//             p.setValue(t.value().value().get_value());
//         } else if (code == cpp2::ErrorCode::E_LEADER_CHANGED) {
//             // retry directly may easily get same error
//             std::this_thread::sleep_for(std::chrono::milliseconds(500));
//             return getValueImpl(spaceId, partId, std::move(key), std::move(p), evb);
//         } else {
//             p.setValue(code);
//         }
//     };

//     getResponse(evb, std::move(req), remote).thenTry(std::move(cb));
// }

}   // namespace storage
}   // namespace nebula
