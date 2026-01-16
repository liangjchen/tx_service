#include "cc/cc_req_base.h"

#include "cc/cc_shard.h"

namespace txservice
{
void CcRequestBase::Free()
{
    if (coro_ != nullptr)
    {
        CcShard *ccs = coro_->GetCcShard();
        ccs->RecycleCcCoro(std::move(coro_));
    }
    in_use_.store(false, std::memory_order_release);
}
}  // namespace txservice