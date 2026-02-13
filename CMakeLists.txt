cmake_minimum_required(VERSION 3.20)
project(jolt LANGUAGES CXX)

add_compile_options(-Wall -Wextra -Wpedantic -Wno-unused-parameter)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif ()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS_RELEASE
        "-O3 -g -march=native -flto -ffast-math -funroll-loops -mavx2 -fno-omit-frame-pointer")

find_package(Threads REQUIRED)
find_library(URING_LIBRARY uring REQUIRED)

set(COMMON_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(Client
        client/ClientMain.cpp
        client/FixClient.cpp
        client/FixClient.h
        market_data_gateway/MarketDataTypes.h
        client/MarketMaker.cpp
        client/MarketMaker.h
        client/NoiseTrader.cpp
        client/NoiseTrader.h
        client/MomemtumTrader.cpp
        client/MomemtumTrader.h
)
target_include_directories(Client PRIVATE ${COMMON_INCLUDE_DIR})

add_executable(EntryGateway
        entry_gateway/GatewayMain.cpp
        entry_gateway/FixGateway.cpp
        entry_gateway/FixGateway.h
        entry_gateway/EventLoop.cpp
        entry_gateway/EventLoop.h
        entry_gateway/FixSession.cpp
        entry_gateway/FixSession.h
        entry_gateway/Client.cpp
        entry_gateway/Client.h
        entry_gateway/GatewayTypes.h
)
target_include_directories(EntryGateway PRIVATE ${COMMON_INCLUDE_DIR})
target_link_libraries(EntryGateway PRIVATE Threads::Threads)
target_compile_options(EntryGateway PRIVATE -mavx2)

add_executable(Exchange
        exchange/ExchangeMain.cpp
        exchange/ExchangeMain.h
        exchange/Exchange.cpp
        exchange/Exchange.h
        exchange/DayTicker.cpp
        exchange/DayTicker.h
        risk/RiskEngine.cpp
        risk/RiskEngine.h
)
target_include_directories(Exchange PRIVATE ${COMMON_INCLUDE_DIR})
target_link_libraries(Exchange PRIVATE Threads::Threads ${URING_LIBRARY})
target_compile_options(Exchange PRIVATE -mavx2)

add_executable(MarketDataGateway
        market_data_gateway/MarketDataGatewayMain.cpp
        market_data_gateway/MarketDataGatewayMain.h
        market_data_gateway/MarketDataGateway.cpp
        market_data_gateway/MarketDataGateway.h
        market_data_gateway/ControlEventLoop.cpp
        market_data_gateway/ControlEventLoop.h
        market_data_gateway/FixControlSession.cpp
        market_data_gateway/FixControlSession.h
        market_data_gateway/RecoverySever.cpp
        market_data_gateway/RecoverySever.h
        market_data_gateway/UdpSever.cpp
        market_data_gateway/UdpSever.h
        market_data_gateway/MarketDataTypes.h
)
target_include_directories(MarketDataGateway PRIVATE ${COMMON_INCLUDE_DIR})
target_link_libraries(MarketDataGateway PRIVATE Threads::Threads)
target_compile_options(MarketDataGateway PRIVATE -mavx2)
