#pragma once
#include <glog/logging.h>

#include <array>
#include <cstddef>
#include <memory_resource>

namespace txservice
{
class CoroSharedAllocator : public std::pmr::memory_resource
{
public:
    using pointer = void *;
    using const_pointer = const void *;
    using size_type = size_t;
    using difference_type = std::ptrdiff_t;

    struct Chunk
    {
        Chunk *next;
    };

    // Pool sizes in bytes
    static constexpr std::array<size_type, 4> POOL_SIZES = {
        4096, 16384, 65536, 262144};

    // Constructor
    CoroSharedAllocator() noexcept
    {
        free_lists_.fill(nullptr);
    }

    pointer allocate(size_type bytes)
    {
        LOG(INFO) << "Allocating " << bytes << " bytes";
        int pool_idx = get_pool_index(bytes);

        if (pool_idx >= 0)
        {
            size_type chunk_size = POOL_SIZES[pool_idx];
            if (free_lists_[pool_idx] == nullptr)
            {
                // Allocate a new chunk for this pool
                void *chunk_mem = (void *) malloc(chunk_size);
                free_lists_[pool_idx] = static_cast<Chunk *>(chunk_mem);
                free_lists_[pool_idx]->next = nullptr;
            }
            // Pop from free list
            Chunk *chunk = free_lists_[pool_idx];
            free_lists_[pool_idx] = free_lists_[pool_idx]->next;
            return reinterpret_cast<pointer>(chunk);
        }
        else
        {
            // Fallback to system allocator for large allocations
            return static_cast<pointer>(malloc(bytes));
        }
    }

    void deallocate(pointer p, size_type bytes)
    {
        LOG(INFO) << "Deallocating " << bytes << " bytes";
        int pool_idx = get_pool_index(bytes);

        if (pool_idx >= 0)
        {
            Chunk *chunk = reinterpret_cast<Chunk *>(p);
            chunk->next = free_lists_[pool_idx];
            free_lists_[pool_idx] = chunk;
        }
        else
        {
            free(p);
        }
    }

    ~CoroSharedAllocator()
    {
        // Free all chunks in each pool
        for (size_t i = 0; i < POOL_SIZES.size(); ++i)
        {
            size_t cnt = 0;
            Chunk *chunk = free_lists_[i];
            while (chunk != nullptr)
            {
                cnt++;
                Chunk *next = chunk->next;
                free(static_cast<void *>(chunk));
                chunk = next;
            }
            free_lists_[i] = nullptr;
            if (cnt > 0)
            {
                LOG(INFO) << "Freed " << cnt << " chunks of size "
                          << POOL_SIZES[i];
            }
        }
    }

protected:
    void *do_allocate(size_t bytes, size_t alignment) override
    {
        // Alignment is ignored for simplicity, but can be handled if needed
        return allocate(bytes);
    }

    void do_deallocate(void *p, size_t bytes, size_t alignment) override
    {
        // Alignment is ignored for simplicity, but can be handled if needed
        deallocate(p, bytes);
    }

    bool do_is_equal(
        const std::pmr::memory_resource &other) const noexcept override
    {
        return this == &other;
    }

private:
    // Returns the pool index for the given size, or -1 if too large
    int get_pool_index(size_type bytes) const
    {
        for (int i = 0; i < static_cast<int>(POOL_SIZES.size()); ++i)
        {
            if (bytes <= POOL_SIZES[i])
                return i;
        }
        return -1;
    }

    std::array<Chunk *, POOL_SIZES.size()> free_lists_;
};
}  // namespace txservice
