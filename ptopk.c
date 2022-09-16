#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <sys/stat.h>

long start_time;
int buffer_size;
int array_size, n;
int * counter;
int * num;
int thread_num;
int ** count;
int num_of_files;

struct Node {
    char fname[255];
    struct Node * next;
};

struct thread_arg {
    int part;
    int size;
    struct Node * head;
};

struct topk_thread_arg {
    int start;
    int end;
};

typedef struct thread_arg ThreadArg;
typedef struct topk_thread_arg TopkThreadArg;

// Function to read each chunk of file
void* ReadChunk(void* arg) {
    ThreadArg * arg_value = (ThreadArg*)arg;
    char buffer[buffer_size + 1];

    struct Node * head = arg_value->head;
    int blocksize = arg_value->size;

    // Reading all files in thread
    while (head != NULL && blocksize > 0) {
    
        FILE * input = fopen(head->fname, "r");

        if (input) {
            while (fgets(buffer, buffer_size, input) != NULL) {
                // Converting and calculating frequencies of each log
                int time_stamp = buffer[0] - '0';

                for (int i = 1; i < 8; i ++)
                    time_stamp = time_stamp * 10 + (buffer[i] - '0');
                
                int temp = (int)(time_stamp - start_time);
                temp /= 36;
                count[arg_value->part][temp] += 1;
            }
            
            fclose(input);
        }
        
        head = head->next;
        blocksize --;
    }

    return 0;
}

int isFile(const char * name) {
    DIR* directory = opendir(name);

    if(directory != NULL) {
         closedir(directory);
         return 0;
    }

    if(errno == ENOTDIR) return 1;
    return -1;
}

void start_threads(int thread_num, struct Node * head){
    // Creating thread and agruments
    ThreadArg arg_list[thread_num];
    pthread_t readers[thread_num];

    int blocksize = (num_of_files + thread_num - 1) / thread_num;

    for (int i = 0; i < thread_num - 1; i ++) {
        arg_list[i].head = head;
        arg_list[i].part = i;
        arg_list[i].size = blocksize;

        int temp_block = blocksize;
        while (head != NULL && temp_block --) {
            head = head->next;
        }
    }

    arg_list[thread_num - 1].head = head;
    arg_list[thread_num - 1].part = thread_num - 1;
    arg_list[thread_num - 1].size = num_of_files - blocksize * (thread_num - 1);
        
    for (int i = 0; i < thread_num; i ++) {
        pthread_create(&readers[i], NULL, ReadChunk, &arg_list[i]);
    }
    
    for (int i = 0; i < thread_num; i ++) {
        pthread_join(readers[i], NULL);
    }
}

void swap(int *a, int *b) {
    int temp = *a;
    *a = *b;
    *b = temp;
}

void heapify(int root, int start, int end, int* a, int *num) {
    int left = (root - start) * 2 + 1 + start;
    int right = (root - start) * 2 + 2 + start;
    int cur = root;

    if (left < end && left >= start && a[left] > a[root]) {
        cur = left;
    } else if (left < end && left >= start && a[left] == a[root] && num[left] > num[root]) {
        cur = left;
    }
    
    if (right < end && right >= start && a[cur] < a[right]) {
        cur = right;
    } else if (right < end && right >= start && a[cur] == a[right] && num[cur] < num[right]) {
        cur = right;
    }

    if (cur != root) {
        swap(&a[cur], &a[root]);
        swap(&num[cur], &num[root]);
        
        heapify(cur, start, end, a, num);
    }
}

void * RunHeap(void * arg) {
    TopkThreadArg * arg_value = (TopkThreadArg*)arg;
    
    int nn = (arg_value->end - arg_value->start) / 2 + arg_value->start;
    while (nn-- > arg_value->start) {
        heapify(nn, arg_value->start, arg_value->end, counter, num);
    }
    
    return 0;
}

void run_topk_threads(int k) {
    // Creating thread and agruments
    TopkThreadArg arg_list[thread_num];
    pthread_t threads[thread_num];
    
    int blocksize = (array_size + thread_num - 1) / thread_num;
    int start_pos = 0;
    
    for (int i = 0; i < thread_num; i ++) {
        arg_list[i].start = start_pos;
        start_pos += blocksize;
        arg_list[i].end = (i == thread_num - 1 ? array_size : start_pos);
    }
    
    for (int i = 0; i < thread_num; i ++) {
        pthread_create(&threads[i], NULL, RunHeap, &arg_list[i]);
    }
    
    for (int i = 0; i < thread_num; i ++) {
        pthread_join(threads[i], NULL);
    }
    
    puts("Top K frequently accessed hour:");
    
    for (int i = 0; i < k; i ++) {
        int mx_count = 0;
        int mx_num = 0;
        int pos = -1;
        
        for (int j = 0; j < thread_num; j ++) {
            if (mx_count < counter[arg_list[j].start]) {
                mx_count = counter[arg_list[j].start];
                pos = j;
                mx_num = num[arg_list[j].start];
            } else if (mx_count == counter[arg_list[j].start] && mx_num < num[arg_list[j].start]) {
                mx_count = counter[arg_list[j].start];
                pos = j;
                mx_num = num[arg_list[j].start];
            }
        }
        long tm = mx_num * 3600 + start_time * 100;
        char * str = asctime(localtime(&tm));
        str[strlen(str)-1] = '\0';
        printf("%s\t%d\n", str, mx_count);
        
        int start_pos = arg_list[pos].start;
        counter[start_pos] = -100;
        heapify(start_pos, start_pos, arg_list[pos].end, counter, num);
    }
}

int main(int argc, const char * argv[]) {
    errno = 0;
    
    // Extracting arguments
    const int k = atoi(argv[3]);
    
    // Initializing const values
    buffer_size = 40;
    array_size = 9321;
    start_time = 16454916;
    thread_num = 4;
    
    // Creating dynamic arrays
    num = (int*)malloc(array_size * sizeof(int));
    counter = (int *)malloc(array_size * sizeof(int));
    count = (int **)malloc(thread_num * sizeof(int));

    for (int i = 0; i < thread_num; i ++)
        count[i] = (int*)malloc(array_size*sizeof(int));

    
    DIR* FD;
    struct dirent* in_file;
    char target_file[255];
    
    struct Node * head = (struct Node *)malloc(sizeof(struct Node));
    struct Node * temp_head = head;
    
    int input_is_file = isFile(argv[1]);
    
    if (input_is_file == 1) {
        strcpy(temp_head->fname, argv[1]);
        temp_head->next = (struct Node *)malloc(sizeof(struct Node));
        temp_head = temp_head->next;
    }
    if (input_is_file == 0) {
        FD = opendir(argv[1]);
        
        if (FD == NULL) {
            fprintf(stderr, "Error : Failed to open input directory - %s\n", strerror(errno));
            return 1;
        }
        
        while ((in_file = readdir(FD)) != NULL) {
            if (!strcmp(in_file->d_name, ".")) continue;
            if (!strcmp(in_file->d_name, "..")) continue;
            strcpy(target_file,argv[1]);
            strcat(target_file,in_file->d_name);
            
            strcpy(temp_head->fname, target_file);
            temp_head->next = (struct Node *)malloc(sizeof(struct Node));
            temp_head = temp_head->next;
            num_of_files ++;
        }
    }
    
    start_threads(thread_num, head);
    
    // Joining frequencies
    for (int i = 0; i < array_size; i ++) {
        num[i] = i;
        counter[i] = 0;
        for (int j = 0; j < thread_num; j ++)
            counter[i] += count[j][i];
    }
    
    // Runing multithreaded sorting and printf TopK elements
    run_topk_threads(k);

    return 0;
}