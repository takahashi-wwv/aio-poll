cmake_minimum_required(VERSION 2.6)
project(aio_poll)

add_executable(aio_poll aio_poll.c)
target_link_libraries(aio_poll aio)

install(TARGETS aio_poll RUNTIME DESTINATION bin)
