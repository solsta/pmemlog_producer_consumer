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

#define POOL_SIZE 8000000
#define MAX_OPS_IN_THE_LOG 50

PMEMlogpool *plp_dest;
PMEMlogpool *plp_src;

void log_commit_flag(char *command){
    if (pmemlog_append(plp_dest, command, strlen(command)) < 0) {
        perror("pmemlog_append");
        exit(1);
    }
}

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
    int commited_Records = 0;
    char tmp;
    memcpy(&tmp, buf, 1);
    int num_of_elements = 0;
    while(tmp == '1'){
        num_of_elements = num_of_elements+1;
        memcpy(&tmp, buf+num_of_elements, 1);

    }
    commited_Records = num_of_elements;
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

    //char *directory;
    //directory = "/mnt/dax/test_outputs/pmem_logs/psm";
    size_t nbyte;
    //char *str;

    //char *path = calloc(strlen(file_name)+strlen(directory), 1);
    //strcat(path, directory);
    //strcat(path, file_name);

    /* create the pmemlog pool or open it if it already exists */
    PMEMlogpool *plp_dest = pmemlog_create(path, POOL_SIZE, 0666);

    if (plp_dest == NULL)
        plp_dest = pmemlog_open(path);

    if (plp_dest == NULL) {
        perror(path);
        exit(1);
    }

    /* how many bytes does the log hold? */
    nbyte = pmemlog_nbyte(plp_dest);
    printf("log holds %zu bytes\n", nbyte);
    printf("Dest log created\n");
    return plp_dest;
}

void run_producer(){
    plp_src = create_log_file();
    op_count = 0;
    for(int i = 0; i< 200; i++){
        log_command("d");
        log_command ("i");
    }
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

    if(files_in_the_directory <= 1 && !recovering){
        printf("Nothing to consume, waiting\n");
        return NULL;
    }
/*
    char *sequence_number = malloc(3);
    char *new_file_ptr = calloc(strlen(directory_path) + strlen(sequence_number), 1);
    sprintf(sequence_number, "%d",min_index);
    strcat(new_file_ptr, directory_path);
    strcat(new_file_ptr, sequence_number);
*/
    char *sequence_number = malloc(3);
    sprintf(sequence_number, "%d",min_index);
    return sequence_number;
}

char *concat_dir_and_filename(char *dir_name, char *file_name){
   // char *sm_directory = "/mnt/dax/test_outputs/pmem_logs/sm/";
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
    printf("Processing index: %s\n", index);
    //pmemlog_close(plp_src);
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
    printf("About to walk\n");
    pmemlog_walk(plp_src, 0, process_log, NULL);
    printf("Consumer finished\n");
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
    sleep(10);
    run_consumer();
}



int
main(int argc, char *argv[])
{
    recovering = false;
    run_producer();
    //create_dest_log_file(get_lowest_index_that_can_be_processed("/mnt/dax/test_outputs/pmem_logs/sm/"));
    run_consumer();



    /* print the log contents */
    //pmemlog_walk(plp_src, 0, process_log, NULL);
    //pmemlog_walk(plp_dest, 0, printit_dest, NULL);
    //printf("Commited operations %d\n", commited_records);

    //pmemlog_close(plp_src);
    //pmemlog_close(plp_dest);
    return 0;
}