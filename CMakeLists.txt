cmake_minimum_required(VERSION 3.20)

project(dkvs
    VERSION 1.0.0
    DESCRIPTION "Distributed key-value store with Raft consensus, in C++20"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

find_package(Threads REQUIRED)

add_library(dkvs_core
    src/kv_store.cpp
    src/codec.cpp
    src/command.cpp
    src/storage.cpp
    src/net.cpp
    src/raft_msgs.cpp
    src/transport.cpp
    src/raft.cpp
    src/protocol.cpp
    src/server.cpp
)

target_include_directories(dkvs_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_compile_options(dkvs_core
    PRIVATE
        $<$<CXX_COMPILER_ID:Clang,AppleClang,GNU>:-Wall -Wextra -Wpedantic>
)

target_link_libraries(dkvs_core
    PUBLIC
        Threads::Threads
)

add_executable(dkvs-server src/main.cpp)
target_link_libraries(dkvs-server PRIVATE dkvs_core)

add_executable(dkvs-cli src/client_main.cpp)
target_link_libraries(dkvs-cli PRIVATE dkvs_core)

# ---- tests ----
option(DKVS_BUILD_TESTS "Build unit tests" ON)

if(DKVS_BUILD_TESTS)
    include(FetchContent)
    FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.zip
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
    # Keep gtest out of the install and use the same runtime on Windows.
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)

    enable_testing()

    add_executable(dkvs_tests
        tests/kv_store_test.cpp
        tests/codec_test.cpp
        tests/storage_test.cpp
        tests/protocol_test.cpp
        tests/raft_test.cpp
    )
    target_link_libraries(dkvs_tests PRIVATE dkvs_core GTest::gtest_main)
    target_compile_options(dkvs_tests
        PRIVATE
            $<$<CXX_COMPILER_ID:Clang,AppleClang,GNU>:-Wall -Wextra -Wpedantic>
    )

    include(GoogleTest)
    gtest_discover_tests(dkvs_tests)
endif()
