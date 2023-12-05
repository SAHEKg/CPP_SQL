add_library(DB database.h database.cpp)
add_library(SQL_database DB_controller.h DB_controller.cpp)

target_link_libraries(SQL_database DB)