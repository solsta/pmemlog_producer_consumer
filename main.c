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
#define OPERATIONS_TO_PRODUCE 200
#define MAX_INDEX_VALUE 1000
#define SM_PMEM_DIRECTORY "/mnt/dax/test_outputs/pmem_logs/sm/"
#define PSM_PMEM_DIRECTORY "/mnt/dax/test_outputs/pmem_logs/psm/"
#define PMEM_HEAP_FILE "/mnt/dax/test_outputs/mmaped_files/pmem_file_copy"

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
    printf("Logging commit flag\n");
    if(plp_dest == NULL){
        printf("PLP_DEST was null\n");
        exit(1);
    }
    if (pmemlog_append(plp_dest, command, strlen(command)) < 0) {
        perror("pmemlog_append");
        exit(1);
    }
    printf("Logged\n");
}
    //PMEMoid root;
    //struct my_root *rootp;
    /* Change to a safe imlementation */
int to_skip;
bool fail;
static int
process_log(const void *buf, size_t len, void *arg)
{   /* Logic to iterate over each command individually */
    //int skip_records = (int) arg;
    printf("will skip %d operations in this log\n", to_skip);
    char tmp;
    int num_of_elements = 0;
    int operation_count = 0;
    memcpy(&tmp, buf, 1);
    /* Allowed values */
    while(tmp == 'i' || tmp == 'd'){
        if(to_skip > 0 && operation_count <= to_skip) {
            /* Skipping */
            operation_count = operation_count+1;
        } else{
            num_of_elements = num_of_elements + 1;
            memcpy(&tmp, buf + num_of_elements, 1);
            /* Process log entry */
            /* Execute state machine code here */
            /* Add an entry to indicate that record was succesfully consumed. */
            printf("Processed: %d\n", num_of_elements);
            sleep(1);
            log_commit_flag("1");
        }
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
    path = get_next_available_file_name(SM_PMEM_DIRECTORY);
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
    for(int i = 0; i< OPERATIONS_TO_PRODUCE; i++){
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
    int min_index = MAX_INDEX_VALUE;
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
        /* Try opening a file and if this fails - wait */
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
    char *index;
    index = get_lowest_index_that_can_be_processed(SM_PMEM_DIRECTORY);
    if (index == NULL){
        sleep(1);
        run_consumer();
    }
    /* Process */
    char *plp_src_path = concat_dir_and_filename(SM_PMEM_DIRECTORY, index);
    plp_src = pmemlog_create(plp_src_path, POOL_SIZE, 0666);
    if (plp_src == NULL)
        plp_src = pmemlog_open(plp_src_path);

    if (plp_src == NULL) {
        perror(plp_src_path);
        exit(1);
    }
    char *plp_dest_path = concat_dir_and_filename(PSM_PMEM_DIRECTORY, index);
    plp_dest = create_dest_log_file(plp_dest_path);

    pmemlog_walk(plp_src, 0, process_log, NULL);

    /* Delete */
    pmemlog_close(plp_src);
    pmemlog_close(plp_dest);

    if (remove(plp_src_path) == 0){
        printf("%s was deleted\n", plp_src_path);
    } else{
        printf("Could not delete\n");
    }
    if (remove(plp_dest_path) == 0){
        printf("%s was deleted\n", plp_dest_path);
        /* Once we delete the log file, there is no need to keep track of committed records,
         * so psm log file can also be deleted.
         * */
    } else{
        printf("Could not delete\n");
    }
    run_consumer();
}

int last_operation_status_stub(){
    return 0;
}

void execute_in_recovery_mode(){
    /* Get number of committed messages */
    /* Iterate over each file in the directory and count commit messages */
    char *index;
    commit_flags = 0;
    index = get_lowest_index_that_can_be_processed(SM_PMEM_DIRECTORY);
    char *file_path = concat_dir_and_filename(PSM_PMEM_DIRECTORY, index);
    printf("PATH: %s\n", file_path);
    //PMEMlogpool *plp_dest = pmemlog_open(file_path);
    plp_dest = pmemlog_open(file_path);
    if (plp_dest != NULL){
        pmemlog_walk(plp_dest, 0, printit_dest, NULL);
        printf("Committed messages: %d\n", commit_flags);

        /* TODO */
        /* Add required logic to run consumer */
        /* Get status of the last operation */
        int commit_last_operation = last_operation_status_stub();
        printf("Last operation status: %d\n", commit_last_operation);
        /* add them up */
        to_skip = commit_flags + commit_last_operation;
        printf("Messages left to process: %d\n", MAX_OPS_IN_THE_LOG - to_skip);
        /* start consumer and ignore the first x operations + write to the logname with _rec extension*/
        printf("Starting log consumption:\n");
        /* Open the log file */
        plp_src = pmemlog_open(concat_dir_and_filename(SM_PMEM_DIRECTORY, index));
        pmemlog_close(plp_dest);
        /* Open another psm file */
        char *rec_file = concat_dir_and_filename(PSM_PMEM_DIRECTORY, index);
        char *rec_file_path = concat_dir_and_filename(rec_file, "_rec");
        plp_dest = pmemlog_create(rec_file_path, POOL_SIZE, 0666);
        if(plp_dest == NULL){
          perror("Failed to create a destination pool\n");
        }
        /* ^ To complete */
        pmemlog_walk(plp_src, 0, process_log, NULL);
    } else{
        /* Run in a normal mode from the current log file */
        printf("Starting normal log processing\n");
        run_consumer();
    }
}

int
main(int argc, char *argv[])
{

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
            fail = false;
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