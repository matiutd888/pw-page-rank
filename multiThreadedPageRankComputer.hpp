#ifndef SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_
#define SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "immutable/network.hpp"
#include "immutable/pageIdAndRank.hpp"
#include "immutable/pageRankComputer.hpp"

class MultiThreadedPageRankComputer : public PageRankComputer {
private:
    class Barrier {
    public:
        Barrier(uint32_t num_threads)
            : num_threads(num_threads)
            , counter(num_threads)
            , generation(0)
            , result_double(0)
            , count_double(0)
        {
        }

        void notify_all()
        {
            result_double = count_double;
            count_double = 0;
            generation = !generation;
            counter = num_threads;
            cv.notify_all();
        }

        void wait()
        {
            std::unique_lock<std::mutex> lock(mutex);
            bool my_gen = generation;
            counter--;
            if (counter == 0) {
                notify_all();
            } else {
                cv.wait(lock, [&] { return my_gen != generation; });
            }
        }
        // Standard wait except while waiting it does a sumation of
        // all arguments ("diffs") and returns the sum.
        double wait_and_sum(double diff)
        {
            std::unique_lock<std::mutex> lock(mutex);
            bool my_gen = generation;
            count_double += diff;
            counter--;
            if (counter == 0) {
                notify_all();
            } else {
                cv.wait(lock, [&] { return my_gen != generation; });
            }
            return result_double;
        }

    private:
        std::mutex mutex;
        std::condition_variable cv;
        uint32_t num_threads;
        uint32_t counter;
        bool generation;
        double result_double;
        double count_double;
    };

public:
    MultiThreadedPageRankComputer(uint32_t numThreadsArg)
        : numThreads(numThreadsArg) {};

    std::vector<PageIdAndRank>
    computeForNetwork(Network const& network, double alpha, uint32_t iterations, double tolerance) const
    {

        size_t num_pages = network.getPages().size();

        std::vector<std::thread> threads(numThreads);
        Barrier barrier(numThreads);

        std::unordered_map<PageId, size_t, PageIdHash> num_links;
        std::unordered_map<PageId, std::vector<PageId>, PageIdHash> edges;
        std::unordered_map<PageId, PageRank, PageIdHash> prev_p_ranks;
        std::unordered_map<PageId, PageRank, PageIdHash> curr_p_ranks;
        std::mutex map_mutex;
        bool success;

        auto thread_func = [&](size_t start, size_t end) {
            const std::vector<Page>& pages = network.getPages();

            for (size_t i = start; i < end; ++i)
                pages[i].generateId(network.getGenerator());

            barrier.wait();

            for (size_t i = start; i < end; ++i) {
                const auto& page_id = pages[i].getId();
                std::lock_guard<std::mutex> map_guard(map_mutex);
                prev_p_ranks[page_id] = 1.0 / (pages.size());
                curr_p_ranks[page_id] = 1.0 / (pages.size());
                num_links[page_id] = pages[i].getLinks().size();

                for (const auto& nei : pages[i].getLinks())
                    edges[nei].push_back(page_id);
            }

            barrier.wait();

            for (uint32_t i = 0; i < iterations; ++i) {
                double m_dangling_addition = 0;

                for (size_t j = start; j < end; j++) {
                    if (pages[j].getLinks().empty())
                        m_dangling_addition += prev_p_ranks[pages[j].getId()];
                }
                double dangling_sum = barrier.wait_and_sum(m_dangling_addition);
                dangling_sum *= alpha;
                double danglingWeight = 1.0 / network.getSize();
                double diff_alpha = (1.0 - alpha) / network.getSize();
                double m_difference_sum = 0;

                for (size_t j = start; j < end; j++) {
                    const auto& page_id = pages[j].getId();
                    double m_sum = dangling_sum * danglingWeight + diff_alpha;
                    if (edges.count(page_id) > 0) {
                        for (const auto& nei : edges.at(page_id))
                            m_sum += alpha * prev_p_ranks[nei] / num_links[nei];
                    }
                    m_difference_sum += std::abs(m_sum - prev_p_ranks.at(page_id));
                    curr_p_ranks[page_id] = m_sum;
                }

                double total = barrier.wait_and_sum(m_difference_sum);
                if (total < tolerance) {
                    success = true;
                    break;
                } else {
                    for (size_t j = start; j < end; j++) {
                        const auto& page_id = pages[j].getId();
                        prev_p_ranks[page_id] = curr_p_ranks[page_id];
                    }
                }
                barrier.wait();
            }
        };

        size_t index = 0;
        for (size_t i = 0; i < numThreads; i++) {
            size_t delta_i = num_pages % numThreads > i ? 1 : 0;
            threads[i] = std::thread(
                thread_func,
                index,
                index + num_pages / numThreads + delta_i);
            index += num_pages / numThreads + delta_i;
        }
        for (auto& t : threads)
            t.join();

        std::vector<PageIdAndRank> result;
        if (success) {
            for (const auto& entry : curr_p_ranks)
                result.push_back(PageIdAndRank(entry.first, entry.second));
            ASSERT(result.size() == network.getSize(), "Invalid result size=" << result.size() << ", for network" << network);
            return result;
        } else {
            ASSERT(false, "Not able to find result in iterations=" << iterations);
        }
    }

    std::string getName() const
    {
        return "MultiThreadedPageRankComputer[" + std::to_string(this->numThreads) + "]";
    }

private:
    uint32_t numThreads;
};

#endif /* SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_ */
