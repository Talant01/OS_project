build: 
	cc ptopk.c -o test.o -l pthread
	cc single_threaded_read.c -o sr.o -l pthread
	cc multi_threaded_read.c -o mr.o -l pthread
	cc single_threaded_read_and_parse.c -o srp.o -l pthread
	cc multi_threaded_read_and_parse.c -o mrp.o -l pthread
test:
	echo "here are the reference output"
	
	./test.o case1/ 1645491600 5
	./test.o case2/ 1645491600 5
	./test.o case3/ 1645491600 5
	./test.o case4/ 1645491600 5
	./test.o case5/ 1645491600 5
	./test.o case6/ 1645491600 5

	echo "here are the reference output"

	./test.example case1/ 1645491600 5
	./test.example case2/ 1645491600 5
	./test.example case3/ 1645491600 5
	./test.example case4/ 1645491600 5
	./test.example case5/ 1645491600 5
	./test.example case6/ 1645491600 5

clean :
	-rm -f *.out *.o
