CC = gcc
CFLAGS = -g -Wall -std=gnu99 -O3
INCLUDES = -I src/
LIBS = -pthread -lrt -lm


all:
	$(CC) $(CFLAGS) $(INCLUDES) -o polyHJ \
	src/util/sys_info.c \
	src/util/cmd_args.c \
	src/util/util.c \
	src/util/threads.c \
	src/util/generate.c \
	src/join/run.c \
	src/join/partition.c \
	src/join/buildprobe_I.c \
	src/join/buildprobe_II.c \
	src/join/buildprobe_III.c \
	src/main.c \
	$(LIBS);
	@echo ""
