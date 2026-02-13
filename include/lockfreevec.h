#include <atomic>
#include <memory>
#include <cstdint>

template <typename T>
class LockFreeVector {
    static constexpr uint32_t MAX_BUCKETS = 32;
    static constexpr uint32_t FIRST_BUCKET_SIZE = 8;
    // holds pending write operations, we use this to ensure a prev write op has completed before starting another one
    struct WriteDescriptor {
        T* loc_;
        T old_val_;
        T new_val_;
        bool completed_;

        WriteDescriptor() : loc_(nullptr), completed_(true) {}

        WriteDescriptor(T* loc, const T& oldVal, const T& newVal)
            : loc_(loc)
            , old_val_(oldVal)
            , new_val_(newVal)
            , completed_(false) {}
    };


    // holds the current state of the vector
    struct Descriptor {
        size_t size_;
        uint32_t counter_;
        WriteDescriptor* pending_write_;

        Descriptor(size_t s = 0, uint32_t c = 0, WriteDescriptor* w = nullptr)
            : size_(s)
            , counter_(c)
            , pending_write_(w) {}
    };

    std::atomic<T*> memory_[MAX_BUCKETS];

    std::atomic<Descriptor*> descriptor_;

public:
    LockFreeVector() {
        descriptor_.store(new Descriptor());

        T* first_bucket = new T[FIRST_BUCKET_SIZE];
        memory_[0].store(first_bucket);

        for (size_t i = 1; i < MAX_BUCKETS; i++) {
            memory_[i].store(nullptr);
        }
    }

    // clz counts num of leading 0s starting from pos 31, substract 31 - num to get msb
    T& at(size_t position) {
        size_t pos = position + FIRST_BUCKET_SIZE;
        size_t hi_bit = __builtin_clz(pos) ^ 31;
        size_t bucket = hi_bit - (__builtin_clz(FIRST_BUCKET_SIZE) ^ 31);
        size_t index = pos ^ (1UL << hi_bit);
        return memory_[bucket].load()[index];
    }

    void allocate_bucket(size_t bucket) {
        size_t bucket_size = FIRST_BUCKET_SIZE * (1 << bucket);
        T* new_bucket = new T[bucket_size];

        T* expected = nullptr;

        // if we already have a bucket at the location we want to allocate, delete
        if (!memory_[bucket].compare_exchange_strong(expected, new_bucket)) {
            delete[] new_bucket;
        }
    }


    void push_back(const T& elem) {
        while (true) {

            Descriptor* current_desc = descriptor_.load();

            if (current_desc->pending_write_) {
                complete_write(current_desc->pending_write_);
            }

            size_t new_size = current_desc->size_ + 1;

            size_t pos = current_desc->size_ + FIRST_BUCKET_SIZE;
            size_t hi_bit = __builtin_clz(pos) ^ 31;
            size_t bucket = hi_bit - (__builtin_clz(FIRST_BUCKET_SIZE) ^ 31);
            size_t index = pos ^ (1UL << hi_bit);

            // allocate a new bucket if needed
            if (!memory_[bucket].load()) {
                allocate_bucket(bucket);
            }

            T* target_loc = &(memory_[bucket].load()[index]);

            // the current write operation we are doing
            WriteDescriptor* write_operation = new WriteDescriptor(target_loc, T(), elem);
            // new descriptor object with the current write op we are doing
            Descriptor* new_desc = new Descriptor(new_size, current_desc->counter_ + 1, write_operation);

            if (descriptor_.compare_exchange_strong(current_desc, new_desc)) {
                complete_write(write_operation);
                break;
            }

            delete write_operation;
            delete new_desc;

        }
    }

    T pop_back() {
        while (true) {

            Descriptor* current_desc = descriptor_.load();
            if (current_desc->pending_write_) {
                complete_write(current_desc->pending_write_);
            }

            if (current_desc->size_ == 0) {
                throw std::out_of_range("empty");
            }

            size_t pos = (current_desc->size_  - 1) + FIRST_BUCKET_SIZE;
            size_t hi_bit = __builtin_clz(pos) ^ 31;
            size_t bucket = hi_bit - (__builtin_clz(FIRST_BUCKET_SIZE) ^ 31);
            size_t index = pos ^ (1UL << hi_bit);

            T* target_addr = &(memory_[bucket].load()[index]);

            T value = *target_addr;

            WriteDescriptor* write_op = new WriteDescriptor(target_addr,
                                                      value,
                                                      T());

            Descriptor* new_desc = new Descriptor(current_desc->size_ - 1, current_desc->counter_ + 1, write_op);

            if (descriptor_.compare_exchange_strong(current_desc, new_desc)) {
                complete_write(write_op);
                return value;
            }

            delete write_op;

            delete new_desc;
        }
    }

    void complete_write(WriteDescriptor* write_op) {
        if (write_op && !write_op->completed_) {
            // get the location of the pending write op
            std::atomic<T>* atomic_loc = reinterpret_cast<std::atomic<T>*>(write_op->loc_);
            T expected = write_op->old_val_;

            if (atomic_loc->compare_exchange_strong(expected, write_op->new_val_)) {
                write_op->completed_ = true;
            }

            // if the cas fails, another thread already completed the write op
            else {
                write_op->completed_ = true;
            }
        }
    }



    T read(const size_t i) { return at(i); }

    void write(const size_t i, const T& elem) {
        size_t pos = i + FIRST_BUCKET_SIZE;
        size_t hi_bit = __builtin_clz(pos) ^ 31;
        size_t bucket = hi_bit - (__builtin_clz(FIRST_BUCKET_SIZE) ^ 31);
        size_t index = pos ^ (1UL << hi_bit);

        T* target = &(memory_[bucket].load()[index]);
        std::atomic<T>* atomic_target = reinterpret_cast<std::atomic<T>*>(target);

        atomic_target->store(elem, std::memory_order_release);
    }


    size_t size() const { return descriptor_.load()->size_; }

};