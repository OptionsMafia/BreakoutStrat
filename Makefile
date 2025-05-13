CC = clang
CFLAGS = -Wall -Wextra -O3 -Wno-unused-parameter
LIBS = -lhiredis -lm -lpthread

# Check for Homebrew hiredis install
HIREDIS_PREFIX = $(shell brew --prefix hiredis 2>/dev/null || echo "/usr/local")

# Add include and library paths for Homebrew-installed packages
CFLAGS += -I$(HIREDIS_PREFIX)/include
LDFLAGS = -L$(HIREDIS_PREFIX)/lib

TARGET = redis_resampler

all: $(TARGET)

$(TARGET): redis_resampler.c
	$(CC) $(CFLAGS) -o $(TARGET) redis_resampler_v2.c $(LDFLAGS) $(LIBS)

clean:
	rm -f $(TARGET)

.PHONY: all clean