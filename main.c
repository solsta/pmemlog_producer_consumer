#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#ifndef _WIN32
#endif
#include <string.h>
#include <libpmemlog.h>
#include <stdbool.h>
#include <dirent.h>
#include <unistd.h>
#include <library.h>
#include <libpmemobj/base.h>
#include <libpmemobj/pool_base.h>
#include <libpmemobj/types.h>

#define POOL_SIZE 8000000
#define MAX_OPS_IN_THE_LOG 110

PMEMlogpool *plp_dest;
PMEMlogpool *plp_src;
int commit_flags;

struct my_root {
    int replay_last_transaction;
    bool tx_is_running;
    int recovery_count;
    long offset;
    char pmem_start[64000000];
};


int last_operation_status(){
    PMEMobjpool *pop;
    PMEMoid root;
    struct my_root *rootp;
    char *path_to_pmem = "/mnt/dax/test_outputs/mmaped_files/pmem_file_copy";
    printf("File name: %s\n", path_to_pmem);
    printf("trying to open file\n");
    if ((pop = pmemobj_open(path_to_pmem, POBJ_LAYOUT_NAME(list))) == NULL) {
        perror("failed to open pool\n");
    }
    root = pmemobj_root(pop, sizeof(struct my_root));
    printf("File opened");
    rootp = pmemobj_direct(root);
    printf("Status of last transaction was: %d\n", rootp->replay_last_transaction);
    int commit_status = rootp->replay_last_transaction;
    pmemobj_close(pop);
    return commit_status;
}

void log_commit_flag(char *command){
    if (pmemlog_append(plp_dest, command, strlen(command)) < 0) {
        perror("pmemlog_append");
        exit(1);
    }
}
    //PMEMoid root;
    //struct my_root *rootp;
    /* Change to a safe imlementation */
static int
process_log(const void *buf, size_t len, void *arg)
{   /* Logic to iterate over each command individually */
    char tmp;
    int num_of_elements = 0;
    memcpy(&tmp, buf, 1);
    /* Allowed values */
    while(tmp == 'i' || tmp == 'd'){

        num_of_elements = num_of_elements+1;
        memcpy(&tmp, buf+num_of_elements, 1);

        /* Process log entry */

        /* Execute state machine code here */

        /* Add an entry to indicate that record was succesfully consumed. */
        log_commit_flag( "1");
    }
    /* File processing is finished, can open next sm logfile and new psm log file */
    return 0;
}


static int
printit_dest(const void *buf, size_t len, void *arg)
{
    printf("Dest walk\n");
    char tmp;
    memcpy(&tmp, buf, 1);
    int num_of_elements = 0;
    while(tmp == '1'){
        num_of_elements = num_of_elements+1;
        memcpy(&tmp, buf+num_of_elements, 1);

    }
    commit_flags = commit_flags + num_of_elements;
    return 0;
}

char *get_next_available_file_name(char *directory_path){
    /* https://stackoverflow.com/questions/4204666/how-to-list-files-in-a-directory-in-a-c-program */
    DIR *directory;
    struct dirent *dir;
    directory = opendir(directory_path);
    int max_index = 0;
    if (directory) {
        while ((dir = readdir(directory)) != NULL) {
            int current_index = atoi(dir->d_name);
            if(max_index < current_index){
                max_index = current_index;
            }
            printf("%s\n", dir->d_name);
        }
        closedir(directory);
    }

    max_index = max_index +1;

    char *sequence_number = malloc(3);
    char *new_file_ptr = calloc(strlen(directory_path) + strlen(sequence_number), 1);
    sprintf(sequence_number, "%d",max_index);
    strcat(new_file_ptr, directory_path);
    strcat(new_file_ptr, sequence_number);

    return new_file_ptr;
}

char *generate_next_sequence_name(){
    /* List files in the directory and get the highest number */
}

PMEMlogpool *create_log_file(){
    char *path;
    /* Change to a unique and increasing name */
    path = get_next_available_file_name("/mnt/dax/test_outputs/pmem_logs/sm/");
    PMEMlogpool *plp;
    /* create the pmemlog pool or open it if it already exists */
    plp = pmemlog_create(path, POOL_SIZE, 0666);

    if (plp == NULL)
        plp = pmemlog_open(path);

    if (plp == NULL) {
        perror(path);
        exit(1);
    }
    return plp;
}

/**
 * Keep track of the entries,
 * once a threshold is reached
 * close current log pool
 * and create a new log pool
 * with a different name.
 **/
int op_count;
void log_command(char *command){
    if(op_count < MAX_OPS_IN_THE_LOG){
        if (pmemlog_append(plp_src, command, strlen(command)) < 0) {
            perror("pmemlog_append");
            exit(1);
        }
        op_count = op_count+1;
    } else{

        /* Create a new log file with a new name and reassign PLP 8 */
        pmemlog_close(plp_src);
        plp_src = create_log_file();
        op_count = 0;
    }

}

PMEMlogpool *create_dest_log_file(char *path){
    size_t nbyte;

    /* create the pmemlog pool or open it if it already exists */
    PMEMlogpool *plp_dest = pmemlog_create(path, POOL_SIZE, 0666);

    if (plp_dest == NULL)
        plp_dest = pmemlog_open(path);

    if (plp_dest == NULL) {
        perror(path);
        exit(1);
    }
    return plp_dest;
}

void run_producer(){
    plp_src = create_log_file();
    op_count = 0;
    for(int i = 0; i< 200; i++){
        log_command("d");
        log_command ("i");
    }
    pmemlog_close(plp_src);
}
bool recovering;
char *get_lowest_index_that_can_be_processed(char *directory_path){
    DIR *directory;
    struct dirent *dir;
    directory = opendir(directory_path);
    int min_index = 1000; //Arbitrary number
    int files_in_the_directory = 0;
    if (directory) {
        while ((dir = readdir(directory)) != NULL) {

            if(strcmp(dir->d_name, ".") != 0 && strcmp(dir->d_name, "..") != 0) {
                int current_index = atoi(dir->d_name);
                files_in_the_directory = files_in_the_directory + 1;
                if (min_index > current_index) {
                    min_index = current_index;
                }
                printf("File: %s\n", dir->d_name);
            }
        }
        closedir(directory);
    }
    printf("Lowest available index is %d\n", min_index);


    char *sequence_number = malloc(3);
    sprintf(sequence_number, "%d",min_index);

    if(files_in_the_directory <= 1 && !recovering){
        printf("Nothing to consume, waiting\n");
        char *last_file = calloc(strlen(directory_path)+strlen(sequence_number), 1);
        strcat(last_file, directory_path);
        strcat(last_file, sequence_number);
        /* Try opening a file and if this fails. than wait */

        plp_src = pmemlog_open(last_file);

        if (plp_src == NULL) {
            printf("Last log file is currently being used\n");
            printf("Last file: %s\n", last_file);
            perror("File open\n");
        } else{
            printf("Consuming the last log file\n");
            pmemlog_close(plp_src);
            return sequence_number;
        }
        return NULL;
    }
    return sequence_number;
}

char *concat_dir_and_filename(char *dir_name, char *file_name){
    char *full_path = calloc(strlen(dir_name)+ strlen(file_name), 1);
    strcat(full_path, dir_name);
    strcat(full_path, file_name);
    return full_path;
}

void run_consumer(){
    /* Check if
     * Set up state on pmem
     * Look in to directory and find lowest file index
     * Check if it is not the last index
     * */
    char *index;
    index = get_lowest_index_that_can_be_processed("/mnt/dax/test_outputs/pmem_logs/sm/");
    if (index == NULL){
        sleep(1);
        run_consumer();
    }
    /* Process */
    char *plp_src_path = concat_dir_and_filename("/mnt/dax/test_outputs/pmem_logs/sm/", index);
    plp_src = pmemlog_create(plp_src_path, POOL_SIZE, 0666);
    if (plp_src == NULL)
        plp_src = pmemlog_open(plp_src_path);

    if (plp_src == NULL) {
        perror(plp_src_path);
        exit(1);
    }
    char *plp_dest_path = concat_dir_and_filename("/mnt/dax/test_outputs/pmem_logs/psm/", index);
    plp_dest = create_dest_log_file(plp_dest_path);

    pmemlog_walk(plp_src, 0, process_log, NULL);

    /* Delete */
    pmemlog_close(plp_src);
    pmemlog_close(plp_dest);
    //unlink(plp_src_path);

    if (remove(plp_src_path) == 0){
        printf("%s was deleted\n", plp_src_path);
        //unlink(plp_src_path);
    } else{
        printf("Could not delete\n");
    }
    if (remove(plp_dest_path) == 0){
        printf("%s was deleted\n", plp_dest_path);
        /* Once we delete the log file, there is no need to keep track of comited records,
         * so psm log file can also be deleted.
         * */
    } else{
        printf("Could not delete\n");
    }
    //sleep(10);
    run_consumer();
}

int last_operation_status_stub(){
    return 0;
}

void execute_in_recovery_mode(){
    /* Get number of commited messages */
    /* Iterate over each file in the directory and count commit messages */
    char *index;
    commit_flags = 0;
    index = get_lowest_index_that_can_be_processed("/mnt/dax/test_outputs/pmem_logs/sm/");
    char *file_path = concat_dir_and_filename("/mnt/dax/test_outputs/pmem_logs/psm/", index);
    printf("PATH: %s\n", file_path);
    PMEMlogpool *plp_dest = pmemlog_open(file_path);
    if (plp_dest != NULL){
        pmemlog_walk(plp_dest, 0, printit_dest, NULL);
        printf("Commited messages: %d\n", commit_flags);

        int commit_last_operation = last_operation_status_stub();
        printf("Last operation status: %d\n", commit_last_operation);
        int total_skip = commit_flags + commit_last_operation;
        printf("Messages left to process: %d\n", MAX_OPS_IN_THE_LOG - total_skip);
        printf("Starting log consumption:\n");
        /* TODO */
        /* Add required logic to run consumer */
        /* Get status of the last operation */
        /* add them up */
        /* start consumer and ignore the first x operations + write to the logname with _rec extension*/
    } else{
        /* Run in a normal mode from the current log file */
        printf("Starting normal log processing\n");
        run_consumer();
    }
}

bool file_exists(const char *path){
    return access(path, F_OK) != 0;
}


void *create_dummy_pmem_file(){
    static PMEMobjpool *pop;
    static PMEMoid root;
    struct my_root *rootp;

    char *path_to_pmem = "/mnt/dax/test_outputs/mmaped_files/pmem_file_copy";
    printf("File name: %s\n", path_to_pmem);
    printf("trying to open file\n");
    if (file_exists((path_to_pmem)) != 0) {
        if ((pop = pmemobj_create(path_to_pmem, POBJ_LAYOUT_NAME(list),
                                  PMEMOBJ_MIN_POOL, 0666)) == NULL) {
            perror("failed to create pool\n");
        }
    } else {
        if ((pop = pmemobj_open(path_to_pmem, POBJ_LAYOUT_NAME(list))) == NULL) {
            perror("failed to open pool\n");
        }
    }
    root = pmemobj_root(pop, sizeof(struct my_root));
    printf("File opened\n");
    rootp = pmemobj_direct(root);
    printf("Direct\n");
    //D_RW(root)->replay_last_transaction = 1;
    //rootp->replay_last_transaction = 1;
    //rootp->tx_is_running = true;
    //memcpy(rootp->replay_last_transaction, true, sizeof(bool));
    printf("Closing\n");
    //pmemobj_close(pop);
    memcpy(rootp->pmem_start, "true", strlen("true")+1);
    printf("Done\n");

    return rootp->pmem_start;
}

int
main(int argc, char *argv[])
{
    /* Create a dummy file, which holds commit status */
    //char *ptr = create_dummy_pmem_file();

    //printf("Value on pmem is : %s\n", ptr);
    if(argc>1){
        if(strcmp(argv[1], "producer") == 0){
            run_producer();
        } else if (strcmp(argv[1], "consumer_with_pmemlog") == 0){
            run_consumer();
        /* Use another class for this */
        } else if (strcmp(argv[1], "consumer_with_pmemobj") == 0) {
            run_consumer();
        } else if(strcmp(argv[1], "recover") == 0){
            execute_in_recovery_mode();
            /* Find the latest psm file,
             * count it's elements.
             * Open the pmem obj file, which contains head
             * read the status of the last operation.
             * If the operation was commited - increase the number of
             * commited messages by 1.
             * Open sm log and run walk method, while ignoring
             * the number equal to a number of commit markers.
             * */
        }
    }

    recovering = false;
    return 0;
}