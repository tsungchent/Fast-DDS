
cmake_minimum_required(VERSION 2.8.12)

project("ContentFilterExample")

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_EXTENSIONS OFF)

# Find requirements
find_package(fastcdr REQUIRED)
find_package(fastrtps REQUIRED)

add_executable(ContentFilterExample ParameterEventPubSubTypes.cxx ParameterEventPublisher.cxx ParameterEventSubscriber.cxx ParameterEventPubSubMain.cxx ParameterEvent.cxx)
target_link_libraries(ContentFilterExample fastcdr fastrtps)
