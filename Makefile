# run make ASAN=1 all etc. to compile with address sanitizers
ASAN_SUFFIX := 
ifeq ($(ASAN),1)
	ASAN_SUFFIX := -fsanitize=address
endif

workload: int_workload_gen.cpp str_workload_gen.cpp
	g++ -O3 -o int_workload int_workload_gen.cpp -Wall -Wextra --std=c++14 -lstdc++fs $(ASAN_SUFFIX)
	g++ -O3 -o str_workload str_workload_gen.cpp -Wall -Wextra --std=c++14 -lstdc++fs $(ASAN_SUFFIX)

clean:
	rm -f int_workload str_workload