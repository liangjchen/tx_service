/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under either of the following two licenses:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *    2. GNU General Public License as published by the Free Software
 *    Foundation; version 2 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License or GNU General Public License for more
 *    details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    and GNU General Public License V2 along with this program.  If not, see
 *    <http://www.gnu.org/licenses/>.
 *
 */
#pragma once

#pragma once

#include <atomic>

#include "cc_coro.h"
#include "cc_protocol.h"
#include "error_messages.h"
#include "tx_id.h"
#include "type.h"

namespace txservice
{
class CcShard;

struct CcRequestBase
{
public:
    virtual ~CcRequestBase() = default;

    /**
     * @brief Processes the cc request toward the input concurrency control (cc)
     * shard.
     *
     * @param ccs The cc shard on which the cc request is processed.
     * @return true, if the request needs to be freed and recycled; false, if
     * the request should not be freed and recycled.
     */
    virtual bool Execute(CcShard &ccs) = 0;

    bool InUse() const
    {
        return in_use_.load(std::memory_order_acquire);
    }

    virtual void Free();

    void Use()
    {
        in_use_.store(true, std::memory_order_release);
    }

    TxNumber Txn() const
    {
        return tx_number_;
    }

    CcProtocol Protocol() const
    {
        return proto_;
    }

    IsolationLevel Isolation() const
    {
        return isolation_level_;
    }

    // Remember to call Free() when implement AbortCcRequest in case it may be
    // recycled in CcRequestPool
    virtual void AbortCcRequest(CcErrorCode err_code)
    {
        Free();
        assert(false && "Unimplemented virtual method");
    }

    virtual uint64_t SchemaVersion() const
    {
        return 0;
    }

    void SetCcCoro(CcCoro::uptr coro)
    {
        coro_ = std::move(coro);
    }

    CcCoro::uptr GetCcCoro()
    {
        return std::move(coro_);
    }

protected:
    CcRequestBase() = default;

    TxNumber tx_number_{0};
    std::atomic<bool> in_use_{false};
    CcProtocol proto_{CcProtocol::OCC};
    IsolationLevel isolation_level_{IsolationLevel::ReadCommitted};
    CcCoro::uptr coro_{nullptr};
};
}  // namespace txservice
