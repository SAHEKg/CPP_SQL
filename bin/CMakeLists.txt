add_executable(main main.cpp)

target_include_directories(main PRIVATE ${PROJECT_SOURCE_DIR})
target_link_libraries(main SQL_database)