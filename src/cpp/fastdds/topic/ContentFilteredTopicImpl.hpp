// Copyright 2021 Proyectos y Sistemas de Mantenimiento SL (eProsima).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * @file ContentFilteredTopicImpl.hpp
 */

#ifndef _FASTDDS_TOPIC_CONTENTFILTEREDTOPICIMPL_HPP_
#define _FASTDDS_TOPIC_CONTENTFILTEREDTOPICIMPL_HPP_

#include <string>

#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/rtps/writer/IReaderDataFilter.hpp>

#include <fastdds/topic/TopicDescriptionImpl.hpp>
#include <fastdds/topic/TopicImpl.hpp>

namespace eprosima {
namespace fastdds {
namespace dds {

namespace poc {
#include "content-filter-poc/ParameterEvent.h"
}  // namespace poc

class ContentFilteredTopicImpl : public TopicDescriptionImpl, public eprosima::fastdds::rtps::IReaderDataFilter
{
public:

    virtual ~ContentFilteredTopicImpl()
    {
        if (nullptr != data_)
        {
            auto related_impl = static_cast<TopicImpl*>(related_topic->get_impl());
            const TypeSupport& type = related_impl->get_type();
            type->deleteData(data_);
            data_ = nullptr;
        }
    }

    const std::string& get_rtps_topic_name() const override
    {
        return related_topic->get_name();
    }

    bool is_relevant(
            const fastrtps::rtps::CacheChange_t& change,
            const fastrtps::rtps::GUID_t& reader_guid) const override
    {
        (void)reader_guid;

        // Any expression different from the PoC ones will pass the filter
        switch (parameters.size())
        {
            default:
                return true;
            case 1:
                if (0 != expression.compare("node = %0"))
                {
                    return true;
                }
                break;
            case 2:
                if (0 != expression.compare("node = %0 AND changed_parameters[0].name = %1"))
                {
                    return true;
                }
                break;
        }

        // Check minimum length
        uint32_t min_len =
                change.serializedPayload.representation_header_size + // RTPS serialized payload header
                4 + 4 +                                               // stamp
                4;                                                    // node (string CDR length)
        if (min_len >= change.serializedPayload.length)
        {
            // Filtered field not present => filter should pass
            return true;
        }

        if (parameters.size() == 1)
        {
            eprosima::fastrtps::rtps::CDRMessage_t msg(change.serializedPayload);
            msg.pos = min_len - 4;
            std::string data_field;

            if (!eprosima::fastrtps::rtps::CDRMessage::readString(&msg, &data_field))
            {
                // Malformed string => filter does not pass
                return false;
            }

            return 0 == parameters[0].compare(data_field);
        }

        auto related_impl = static_cast<TopicImpl*>(related_topic->get_impl());
        const TypeSupport& type = related_impl->get_type();
        if (nullptr == data_)
        {
            data_ = (poc::ParameterEvent*)type->createData();
        }
        type->deserialize(const_cast<fastrtps::rtps::SerializedPayload_t*>(&change.serializedPayload), data_);

        return
            data_->node() == parameters[0] &&
            !data_->changed_parameters().empty() &&
            data_->changed_parameters()[0].name() == parameters[1];
    }

    Topic* related_topic = nullptr;
    std::string expression;
    std::vector<std::string> parameters;

private:

    mutable poc::ParameterEvent* data_ = nullptr;
};

} /* namespace dds */
} /* namespace fastdds */
} /* namespace eprosima */

#endif  // _FASTDDS_TOPIC_CONTENTFILTEREDTOPICIMPL_HPP_
