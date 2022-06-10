#ifndef SRC_SINGLETHREADEDPAGERANKCOMPUTER_HPP_
#define SRC_SINGLETHREADEDPAGERANKCOMPUTER_HPP_

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "immutable/network.hpp"
#include "immutable/pageIdAndRank.hpp"
#include "immutable/pageRankComputer.hpp"

class SingleThreadedPageRankComputer : public PageRankComputer {
public:
    SingleThreadedPageRankComputer() {};

    std::vector<PageIdAndRank>
    computeForNetwork(Network const& network, double alpha, uint32_t iterations, double tolerance) const
    {
        std::unordered_map<PageId, PageRank, PageIdHash> prevHashMap;
        std::unordered_map<PageId, PageRank, PageIdHash> currHashMap;

        auto& pages = network.getPages();
        for (size_t i = 0; i < pages.size(); i++) {
            pages[i].generateId(network.getGenerator());
        }

        std::unordered_map<PageId, uint32_t, PageIdHash> numLinks;
        std::unordered_map<PageId, std::vector<PageId>, PageIdHash> edges;
        for (size_t i = 0; i < pages.size(); i++) {
            const auto& page_id = pages[i].getId();
            prevHashMap[page_id] = 1.0 / network.getSize();
            currHashMap[page_id] = 1.0 / network.getSize();
            numLinks[page_id] = pages[i].getLinks().size();
            for (auto link : pages[i].getLinks()) {
                edges[link].push_back(page_id);
            }
        }

        for (uint32_t i = 0; i < iterations; ++i) {
            double dangleSum = 0;
            for (const auto& page : network.getPages()) {
                if (page.getLinks().empty())
                    dangleSum += prevHashMap[page.getId()];
            }
            dangleSum *= alpha;

            double difference = 0;
            for (size_t j = 0; j < pages.size(); j++) {
                const auto& page_id = pages[j].getId();

                double danglingWeight = 1.0 / network.getSize();
                double m_sum = dangleSum * danglingWeight + (1.0 - alpha) / network.getSize();

                if (edges.count(page_id) > 0) {
                    for (const auto& link : edges[page_id]) {
                        m_sum += alpha * prevHashMap[link] / numLinks[link];
                    }
                }
                difference += std::abs(m_sum - prevHashMap[page_id]);
                currHashMap[page_id] = m_sum;
            }

            if (difference < tolerance) {
                std::vector<PageIdAndRank> result;
                for (const auto& iter : prevHashMap) {
                    result.push_back(PageIdAndRank(iter.first, iter.second));
                }
                ASSERT(result.size() == network.getSize(), "Invalid result size=" << result.size() << ", for network" << network);
                return result;
            } else {
                for (size_t j = 0; j < pages.size(); ++j) {
                    const auto& page_id = pages[j].getId();
                    prevHashMap[page_id] = currHashMap[page_id];
                }
            }
        }

        ASSERT(false, "Not able to find result in iterations=" << iterations);
    }

    std::string getName() const
    {
        return "SingleThreadedPageRankComputer";
    }
};

#endif /* SRC_SINGLETHREADEDPAGERANKCOMPUTER_HPP_ */
