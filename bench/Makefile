# run make ASAN=1 bench etc. to compile with address sanitizers
ASAN_SUFFIX := 
ifeq ($(ASAN),1)
	ASAN_SUFFIX := -fsanitize=address
endif

bench: bench.cpp
	g++ -Ofast -march=native ../include/*.cpp bench.cpp -o bench --std=c++14 -DKEEP_STATS -Wall -Wextra -g $(ASAN_SUFFIX)

clean:
	rm -f bench