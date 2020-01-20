prog: main.c work.c Jobs_Threads.c Statistics.c header.h input_functions.c deletefun.c sort.c print_functions.c insertfun.c filter_join.c
	gcc -o prog main.c work.c Jobs_Threads.c  Statistics.c input_functions.c deletefun.c sort.c print_functions.c insertfun.c filter_join.c -I -Werror -Wall -g -w -O3 -lpthread


clean:
	rm -f *.o project prog

