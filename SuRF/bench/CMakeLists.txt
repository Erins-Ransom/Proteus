add_executable(workload workload.cpp)
target_link_libraries(workload)

add_executable(my_workload my_workload.cpp disk_access.h)
target_link_libraries(my_workload)

add_executable(workload_multi_thread workload_multi_thread.cpp)
target_link_libraries(workload_multi_thread)

#add_executable(workload_arf workload_arf.cpp)
#target_link_libraries(workload_arf ARF)
