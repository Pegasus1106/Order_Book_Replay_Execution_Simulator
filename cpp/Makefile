CXX ?= c++
CXXFLAGS ?= -std=c++17 -O3 -Wall -Wextra -Wno-unused-parameter
LDLIBS = -lz

itch_replay: main.cpp itch.hpp book.hpp
	$(CXX) $(CXXFLAGS) main.cpp -o $@ $(LDLIBS)

# Same code with AddressSanitizer/UBSan, used for the fuzz cross-check.
itch_replay_asan: main.cpp itch.hpp book.hpp
	$(CXX) -std=c++17 -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer main.cpp -o $@ $(LDLIBS)

clean:
	rm -f itch_replay itch_replay_asan

.PHONY: clean
