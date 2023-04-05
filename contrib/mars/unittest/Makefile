all:

CXXFLAGS += -g -O0

##############################################################################
# Google Test
##############################################################################

GTEST := googletest/googletest

gtest_main.a: gtest_main.o gtest-all.o

gtest%.o: CXXFLAGS += -I $(GTEST) -I$(GTEST)/include
gtest%.o: $(GTEST)/src/gtest%.cc
	$(CXX) $(CXXFLAGS) -o $@ $< -c

gtest_%.a:
	$(AR) -r $@ $^

##############################################################################
# The unit tests
##############################################################################

top_builddir = ../../..

CXXFLAGS += -std=c++11
CXXFLAGS += -D_GNU_SOURCE
CXXFLAGS += -I.
CXXFLAGS += -I$(top_builddir)/src/include
CXXFLAGS += -I$(GTEST)/include

CXXFLAGS += $(shell pkg-config --cflags arrow parquet)
LDFLAGS  += $(shell pkg-config --libs   arrow parquet)

LDFLAGS += -pthread

SRCS += ColumnStore_test.cc
SRCS += type_traits_test.cc
SRCS += LogicalRowStore_test.cc
SRCS += ScanKey_test.cc
SRCS += Scanner_test.cc
OBJS := $(SRCS:.cc=.o)

TEST = test

all: unittest

clean:
	-rm -f gtest_main.a
	-rm -f *.o
	-rm -f $(TEST)

unittest: $(TEST)
	./$(TEST) --gtest_color=yes

%_test.o: %_test.cc
	$(CXX) $(CXXFLAGS) -o $@ $< -c

$(TEST): $(OBJS) gtest_main.a
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

.PHONY: all clean unittest
