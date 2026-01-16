#pragma once

#include <generator>
#include <memory>

namespace txservice
{
class CcShard;

class CcCoro
{
public:
    using uptr = std::unique_ptr<CcCoro>;

    CcCoro(CcShard *ccs);

    ~CcCoro();

    bool Resume()
    {
        if (iter_ != coro_.end())
        {
            ++iter_;
            return *iter_;
        }
        else
        {
            return true;
        }
    }

    void Free()
    {
        if (has_stack_)
        {
            coro_.~generator();
            has_stack_ = false;
        }
    }

    bool Start(std::generator<bool> coro)
    {
        if (has_stack_)
        {
            coro_.~generator();
        }

        // The G++ compiler seems to have a bug on std::generator, causing the
        // move assignment to crash. Replaces the move assignment with placement
        // new.
        auto *tmp = new (&coro_) std::generator<bool>(std::move(coro));
        iter_ = tmp->begin();
        has_stack_ = true;

        return *iter_;
    }

    CcShard *GetCcShard()
    {
        return ccs_;
    }

private:
    std::generator<bool> coro_;
    std::ranges::iterator_t<decltype(coro_)> iter_;
    bool has_stack_{false};

    CcCoro::uptr next_{nullptr};
    CcShard *const ccs_;

    friend class CcShard;
};
}  // namespace txservice