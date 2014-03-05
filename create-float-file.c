/*
 * vim: ts=8:sw=8:tw=79:noet
 */

#include <stdio.h>

#define DOUBLE_SIZE sizeof(double)

int main(int argc, char **argv)
{
	int i, num_floats;
	union {
		double d;
		char c[DOUBLE_SIZE];
	} u;

	if (argc != 2) {
		fprintf(stderr, "usage: create-float-file [num-floats]\n");
		return 1;
	}
	num_floats = atoi(argv[1]);
	if (num_floats <= 0) {
		fprintf(stderr, "failed to parse num_floats.\n"
			"usage: create-float-file [num-floats]\n");
		return 1;
	}
	u.d = 0.0;
	for (i = 0; i < num_floats; i++) {
		if (u.d > 100000) u.d = 0.0;
		fwrite(u.c, 1, DOUBLE_SIZE, stdout);
		u.d += 0.5;
	}
	return 0;
}
