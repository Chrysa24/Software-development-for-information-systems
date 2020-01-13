prog: main.c work.c header.h input_functions.c deletefun.c sort.c print_functions.c insertfun.c filter_join.c
	gcc -o prog main.c work.c input_functions.c deletefun.c sort.c print_functions.c insertfun.c filter_join.c -I -Werror -Wall -g -w -O3


clean:
	rm -f *.o project prog

