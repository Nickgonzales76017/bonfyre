# Shared text primitives over utf8proc and RE2, both from the package manager.
CXX ?= c++
CC ?= cc
PKGS = libutf8proc re2
CXXFLAGS ?= -O2 -Wall -Wextra -std=c++17
all: libbonfyre-text.a
libbonfyre-text.a: bf_text.cc bf_text.h
	$(CXX) $(CXXFLAGS) $(shell pkg-config --cflags $(PKGS)) -c bf_text.cc -o bf_text.o
	ar rcs $@ bf_text.o
tests/bf_text_test: tests/bf_text_test.c libbonfyre-text.a
	$(CC) -O2 -Wall -Wextra -std=c11 tests/bf_text_test.c libbonfyre-text.a $(shell pkg-config --libs $(PKGS)) -lc++ -o $@
test: tests/bf_text_test
	./tests/bf_text_test
clean:
	rm -f bf_text.o libbonfyre-text.a tests/bf_text_test
.PHONY: all test clean
