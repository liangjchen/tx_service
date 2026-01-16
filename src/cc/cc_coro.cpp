#include "cc/cc_coro.h"

#include "cc/cc_shard.h"

namespace txservice
{
CcCoro::CcCoro(CcShard *ccs)
    : coro_(
          [](std::allocator_arg_t, std::pmr::polymorphic_allocator<bool> alloc)
              -> std::generator<bool>
          {
              co_yield true;
          }(std::allocator_arg,
            std::pmr::polymorphic_allocator<bool>{ccs->GetSharedAllocator()})),
      iter_(coro_.begin()),
      has_stack_(true),
      ccs_(ccs)
{
}

CcCoro::~CcCoro()
{
    // The G++ compiler seems to have a bug on std::generator such that if
    // the member variable of std::generator has called the deconstructor
    // and freed the stack memory proactively, the destructor of the class
    // will free the std::generator's stack memory again.
    if (!has_stack_)
    {
        new (&coro_)
            std::generator<bool>([](std::allocator_arg_t,
                                    std::pmr::polymorphic_allocator<bool> alloc)
                                     -> std::generator<bool> { co_yield true; }(
                                         std::allocator_arg,
                                         std::pmr::polymorphic_allocator<bool>{
                                             ccs_->GetSharedAllocator()}));
    }
}
}  // namespace txservice