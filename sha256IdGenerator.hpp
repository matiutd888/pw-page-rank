#ifndef SRC_SHA256IDGENERATOR_HPP_
#define SRC_SHA256IDGENERATOR_HPP_

#include "immutable/common.hpp"
#include "immutable/idGenerator.hpp"
#include "immutable/pageId.hpp"
#include <fstream>

class Sha256IdGenerator : public IdGenerator {
public:
    virtual PageId generateId(std::string const& content) const
    {
        FILE* stream = popen(("echo -n \"" + content + "\" | sha256sum").c_str(), "r");
        char buffer[65];
        if (stream == NULL || fgets(buffer, sizeof(buffer), stream) == NULL) {
            ASSERT(false, "ERROR WHILE READING FROM STREAM!");
        }

        pclose(stream);
        return PageId(buffer);
    }
};

#endif /* SRC_SHA256IDGENERATOR_HPP_ */
