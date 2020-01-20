#include "header.h"

extern Job* JobHead; // Head of the Jobs List
extern pthread_mutex_t sjoblist_mut;
extern BUCKET_LIMIT;

void CreateBucketList(bucket **head, int* hist, uint64_t* psum){
	// Create the starting bucket's list based on the hist and psum
	int i, counter = 0, lastBucket = 0;
	bucket *temp = NULL;

	for(i=0; i<256; i++)
		if(hist[i] != 0)
			counter ++;

	for(i=0; i<256; i++){
		if(hist[i] != 0){
			// This is a bucket
			lastBucket++;

			bucket * newnode;
    		newnode = (bucket*)malloc(sizeof(bucket));

			newnode->numofbyte = 0;
			newnode->flag = 0;
			newnode->start = psum[i];
			if(lastBucket == counter)
				newnode->end = newnode->start + hist[i];
			else
				newnode->end = psum[i+1];
			
			if(lastBucket == 1){
				// This is the first bucket created
				*head = newnode;
				temp = newnode;
			}
			else{
				temp->next = newnode;
				temp = temp->next;
			}
		}
	}
	//This is the last node's next
	temp->next = NULL;
}

int DivideBucket(bucket **head, int* hist, uint64_t* psum, bucket *base, int numofbyte){
	// When a bucket is bigger than 64KB it is divided to smaller buckets
	// Base is the pointer that points to this bucket node, a new hist and psum must have been created
	int i, counter = 0, lastBucket = 0, flag = 0;
	bucket *temp = NULL, *previous = *head;

	// If base is the head node
	if(*head == base)
		flag = 1;
	else
		//Find the previous node
		while(previous->next != base)
			previous = previous->next;

	for(i=0; i<256; i++)
		if(hist[i] != 0)
			counter ++;

	for(i=0; i<256; i++){
		if(hist[i] != 0){
			// This is a bucket
			lastBucket++;

			bucket * newnode;
    		newnode = (bucket*)malloc(sizeof(bucket));

			newnode->numofbyte = numofbyte;
			newnode->flag = base->flag+1;
			newnode->start = psum[i] + base->start;
			if(lastBucket == counter)
				newnode->end = base->end;
			else
				newnode->end = psum[i+1] + base->start;
			
			if(lastBucket == 1){
				if(flag == 1)
					// If base was the head
					*head = newnode;
				else
					previous->next = newnode;
				temp = newnode;
			}
			else{
				temp->next = newnode;
				temp = newnode;
			}
		}
	}
	//This is the last node's next
	temp->next = base->next;
	free(base);

	return lastBucket-1;
}

void RemoveBucket(bucket **head, bucket *base){
	// Remove the bucket pointed by the pointer 'base'
    if(base == *head){
        *head = base->next;
        free(base);
        return;
    }
    bucket *temp = *head;
    while(temp->next != base)
        temp = temp->next;
    temp->next = base->next;
    free(base);
}

void ReadFile(char* pathname, uint64_t** array){
	// Read the file and insert elements in the initial Table
	FILE* fp = fopen(pathname,"r");
	if(fp == NULL)
		printf("ERROR: Could not open the file.\n");

	char* line = NULL;
	size_t len = 0;
	uint64_t j = 0;
	
	while(getline(&line, &len, fp) != -1){

		sscanf(line,"%lu,%lu", &array[0][j], &array[1][j]);
		j++;
	}
	free(line);
	fclose(fp);
}

uint64_t CountRows(char* pathname){
	//Return the number of rows of a file
	FILE* fp = fopen(pathname,"r");
	if(fp == NULL)
		printf("ERROR: Could not open the file.\n");

	char* line = NULL;
	size_t len = 0;
	
	uint64_t counter = 0;
	
	while(getline(&line, &len, fp) != -1)
		counter++;

	free(line);
	fclose(fp);
	return counter;
}

void CreateIndexTable(uint64_t** Index, uint64_t** array,uint64_t rows,uint64_t column){
	// Create the Index table
	uint64_t i;
	
	for(i=0 ; i<rows ; i++){
		Index[i][0]=i;						// rowid
		Index[i][1]=array[column][i];	// key
	}
}

void SetZero(int* table, int size){
	//Set a table of size 'size' to zero
	int j;

	for(j=0; j < size; j++)
		table[j] = 0;
}

int GetByte(uint64_t num, int byte){
	//Return the requested byte of a number in decimal
   	return (int)(num>>8*(7-byte))& 255;
}

void CreateHist(uint64_t** Table, int* hist, uint64_t from, uint64_t to, int byte){
	// Create a Table's hist based on the 'byte' number of byte
	uint64_t i;
	SetZero(hist, 256); // Set the previous hist to zero
	int index;
	
   	for(i=from; i<to; i++){
   		index = GetByte(Table[i][1], byte);
   		hist[index]++;
	}
}

void CreatePsum(int *hist, uint64_t *psum){
	// Create the psum table using the given hist
	uint64_t offset = 0;
	int i;
	
   	for(i=0; i<256; i++){
   		psum[i] = offset;
		offset += hist[i];
	}
}

void Reorder(uint64_t** From, uint64_t** To, uint64_t* psum, uint64_t from, uint64_t to, int byte){
	// Sort the contents of the 'From' table to the 'To' table based on the byte number 'byte'
    uint64_t i, position;

    for(i=from; i<to; i++){
        // This is the position of the next item in the To table
        position = from + psum[GetByte(From[i][1], byte)];

        To[position][0] = From[i][0];
        To[position][1] = From[i][1];
        
        // Increase the counter showing the position of the next key
        psum[GetByte(From[i][1], byte)] ++;
    }
}

void CopyBucket(uint64_t** From, uint64_t** To, uint64_t from, uint64_t to){
	// Copy a part of the 'From' table to the 'To' table
	uint64_t i;

	for(i=from; i<to; i++){
		To[i][0] = From[i][0];
		To[i][1] = From[i][1];		
	}
}

uint64_t Partition(uint64_t** array, int64_t start, int64_t end){
	// Used for the quicksort algorithm
	uint64_t part = array[end][1];
	uint64_t temp0,temp1, j, i = start - 1;

	for(j = start; j<= end-1; j++){
		if(array[j][1] <= part){
			i++;
			//Swap
			temp0 = array[i][0];
			temp1 = array[i][1];
			
			array[i][0] = array[j][0];
			array[i][1] = array[j][1];
			
			array[j][0] = temp0;
			array[j][1] = temp1;
		}
	}
	//Swap
	temp0 = array[i+1][0];
	temp1 = array[i+1][1];
	
	array[i+1][0] = array[end][0];
	array[i+1][1] = array[end][1];
	
	array[end][0] = temp0;
	array[end][1] = temp1;
	return i+1;
}

void Myqsort(uint64_t** array, int64_t start, int64_t end){
	// Sort the array from 'start' to 'end'
	uint64_t i;

	if(end <= start)
		return;
		
	i=Partition(array, start, end);
	
	Myqsort(array, start, i-1);
	Myqsort(array, i+1 ,end);
}

void FinalizeTables(uint64_t** Index1, uint64_t** Index2, bucket **head, pthread_cond_t* thread_cond, pthread_mutex_t* thread_mut, int* waiting){
	// Sort the input tables
	bucket *current = *head;
	int hist[256];
	uint64_t psum[256], **From, **To;
	int bucket_counter = 1;

	while(current!=NULL){

		// If the buckets are more than a specific number, give them to the working threads
		if(bucket_counter >= BUCKET_LIMIT){
			
			pthread_mutex_lock(&sjoblist_mut);
			pthread_mutex_lock(thread_mut);
				*waiting = BucketsToJobs(current, JobHead, Index1, Index2, thread_cond, thread_mut, waiting, bucket_counter);
			pthread_mutex_unlock(thread_mut);
			pthread_mutex_unlock(&sjoblist_mut);
				
			//Wait until sorting is done
			pthread_mutex_lock(thread_mut);
				while(*waiting != 0){
					pthread_cond_wait(thread_cond,thread_mut);
				}
			pthread_mutex_unlock(thread_mut);

			return;
		}
		else if(current->flag % 2 == 1){
			From = Index1;
			To = Index2;
		}
		else{
			From = Index2;
			To = Index1;
		}
		
		if(((current->end - current->start)*sizeof(From[0][1]) > 64*1024) && (current->numofbyte < 7)){
			// This bucket is bigger than 64KB and need to be divided
			uint64_t from = current->start, to = current->end;
			int numofbyte = current->numofbyte + 1;

			// Create a hist based on this bucket (hashed on the next byte)
			CreateHist(From, hist, from, to, numofbyte);

			//Create psum
			CreatePsum(hist, psum);

			// Divide the bucket to smaller ones
			bucket_counter += DivideBucket(head, hist, psum, current, numofbyte);
						
			// Reorder this newly made part of the table						
			Reorder(From, To, psum, from, to, numofbyte);
		}
		else{
			if(current->numofbyte < 7)
				Myqsort(From, (int64_t)current->start, (int64_t)current->end-1);

			// It is ready to be copied to the other index table
			CopyBucket(From, To, current->start, current->end);

			RemoveBucket(head, current);
		}
		current = *head;
	}
}

void FinalizeTablesThread(uint64_t** Index1, uint64_t** Index2, bucket **head){
	// Sort the input tables
	bucket *current = *head;
	int hist[256];
	uint64_t psum[256], **From, **To;

	while(current!=NULL){

		if(current->flag % 2 == 1){
			From = Index1;
			To = Index2;
		}
		else{
			From = Index2;
			To = Index1;
		}

		if(((current->end - current->start)*sizeof(From[0][1]) > 64*1024) && (current->numofbyte < 7)){
			// This bucket is bigger than 64KB and need to be divided
			uint64_t from = current->start, to = current->end;
			int numofbyte = current->numofbyte + 1;

			// Create a hist based on this bucket (hashed on the next byte)
			CreateHist(From, hist, from, to, numofbyte);

			//Create psum
			CreatePsum(hist, psum);

			// Divide the bucket to smaller ones
			DivideBucket(head, hist, psum, current, numofbyte);
						
			// Reorder this newly made part of the table						
			Reorder(From, To, psum, from, to, numofbyte);
		}
		else{
			if(current->numofbyte < 7)
				Myqsort(From, (int64_t)current->start, (int64_t)current->end-1);

			// It is ready to be copied to the other index table
			CopyBucket(From, To, current->start, current->end);

			RemoveBucket(head, current);
		}
		current = *head;
	}
}

void CompareTables(uint64_t** Table1,uint64_t** Table2,uint64_t rows, char* name){
	//Test if the output of FinalizeTables is truly a sorted table
	Myqsort(Table1, 0, rows-1);

	uint64_t i;
	for(i=0 ;i< rows ; i++){
		if(Table1[i][1]!=Table2[i][1]){
			printf("* Comparison Failed for %s: Mistake in row: %lu. *\n", name, i);
			printf("%li is not %li\n", Table1[i], Table2[i]);
			return;
		}
	}
	printf("Comparison Successful for %s.\n", name);
}

void Sort(uint64_t** indexA, uint64_t rowsA, pthread_cond_t* thread_cond, pthread_mutex_t* thread_mut, int* waiting){
	// Sorts the given index table
	int hist[256],i;
	uint64_t psum[256];

	// Create hist and psum for Index A
	CreateHist(indexA, hist, 0, rowsA, 0);
	CreatePsum(hist, psum);

	uint64_t** twin;
	// Create its twin table
	twin = malloc( sizeof(uint64_t) * rowsA);
	for(i=0 ; i<rowsA ; i++)
		twin[i]=malloc( sizeof(uint64_t) *2);
	
	// Reorder Index A to twin table
	Reorder(indexA, twin, psum, 0, rowsA, 0);

	// Create psum again because it was ruined from Reorder!
	CreatePsum(hist, psum);

	bucket *head = NULL;
	CreateBucketList(&head, hist, psum);

	// Sort table A
	FinalizeTables(indexA, twin, &head, thread_cond,thread_mut, waiting);

	for(i=0 ; i<rowsA ; i++)
		free(twin[i]);
	free(twin);
}

// T R I P L E    S E C T I O N
void CreateHistTriple(int64_t** Table, int* hist, int64_t from, int64_t to, int byte){
	// Create a Table's hist based on the 'byte' number of byte
	uint64_t i;
	SetZero(hist, 256); // Set the previous hist to zero
	int index;
	
   	for(i=from; i<to; i++){
   		index = GetByte(Table[i][0], byte);
   		hist[index]++;
	}
}

void ReorderTriple(int64_t** From, int64_t** To, uint64_t* psum, int64_t from, int64_t to, int byte, int columns){
	// Sort the contents of the 'From' table to the 'To' table based on the byte number 'byte'
    uint64_t i, position;
	int j;

    for(i=from; i<to; i++){
        // This is the position of the next item in the To table
        position = from + psum[GetByte(From[i][0], byte)];

		for(j=0 ; j<columns ; j++){
			To[position][j] = From[i][j];
		}
        
        // Increase the counter showing the position of the next key
        psum[GetByte(From[i][0], byte)] ++;
    }
}

uint64_t PartitionTriple(int64_t** array, int64_t start, int64_t end, int columns){
	// Used for the quicksort algorithm
	uint64_t part = array[end][0];
	uint64_t *temp, j, i = start - 1;
	int cols;

	temp = malloc(sizeof(uint64_t) * columns);

	for(j = start; j<= end-1; j++){
		if(array[j][0] <= part){
			i++;
			//Swap
			for(cols=0 ; cols<columns ; cols++)
				temp[cols] = array[i][cols];
			
			for(cols=0 ; cols<columns ; cols++)
				array[i][cols] = array[j][cols];
			
			for(cols=0 ; cols<columns ; cols++)
				array[j][cols] = temp[cols];
		}
	}
	//Swap
	for(cols=0 ; cols<columns ; cols++)
		temp[cols] = array[i+1][cols];
	
	for(cols=0 ; cols<columns ; cols++)
		array[i+1][cols] = array[end][cols];
	
	for(cols=0 ; cols<columns ; cols++)
		array[end][cols] = temp[cols];

	free(temp);
	return i+1;
}

void MyqsortTriple(int64_t** array, int64_t start, int64_t end, int columns){
	// Sort the array from 'start' to 'end'
	uint64_t i;

	if(end <= start)
		return;
		
	i=PartitionTriple(array, start, end, columns);
	
	MyqsortTriple(array, start, i-1, columns);
	MyqsortTriple(array, i+1 ,end, columns);
}

void CopyBucketTriple(int64_t** From, int64_t** To, uint64_t from, uint64_t to, int columns){
	// Copy a part of the 'From' table to the 'To' table
	uint64_t i;
	int j;

	for(i=from; i<to; i++){
		for(j=0 ; j<columns ;j++){
			To[i][j] = From[i][j];
		}	
	}
}

void FinalizeTablesTriple(int64_t** Index1, int64_t** Index2, bucket **head, int columns, pthread_cond_t* thread_cond, pthread_mutex_t* thread_mut, int* waiting){
	// Sort the input tables
	bucket *current = *head;
	int hist[256];
	uint64_t psum[256];
	int64_t **From, **To;
	int bucket_counter = 1;

	while(current!=NULL){

		// If the buckets are more than a specific number, give them to the working threads
		if(bucket_counter >= BUCKET_LIMIT){

			pthread_mutex_lock(&sjoblist_mut);
			pthread_mutex_lock(thread_mut);
				*waiting = BucketsToJobsTriple(current, JobHead, Index1, Index2, columns, thread_cond, thread_mut, waiting, bucket_counter);
			pthread_mutex_unlock(thread_mut);
			pthread_mutex_unlock(&sjoblist_mut);
				
			//Wait until sorting is done
			pthread_mutex_lock(thread_mut);
				while(*waiting != 0){
					pthread_cond_wait(thread_cond,thread_mut);
				}
			pthread_mutex_unlock(thread_mut);		

			return;
		}
		else if(current->flag % 2 == 1){
			From = Index1;
			To = Index2;
		}
		else{
			From = Index2;
			To = Index1;
		}

		if(((current->end - current->start)*sizeof(From[0][1]) > 64*1024) && (current->numofbyte < 7)){
			// This bucket is bigger than 64KB and need to be divided
			int64_t from = current->start, to = current->end;
			int numofbyte = current->numofbyte + 1;

			// Create a hist based on this bucket (hashed on the next byte)
			CreateHistTriple(From, hist, from, to, numofbyte);

			//Create psum
			CreatePsum(hist, psum);

			// Divide the bucket to smaller ones
			bucket_counter += DivideBucket(head, hist, psum, current, numofbyte);
						
			// Reorder this newly made part of the table						
			ReorderTriple(From, To, psum, from, to, numofbyte, columns);
		}
		else{
			if(current->numofbyte < 7)
				MyqsortTriple(From, (int64_t)current->start, (int64_t)current->end-1, columns);

			// It is ready to be copied to the other index table
			CopyBucketTriple(From, To, current->start, current->end, columns);

			RemoveBucket(head, current);
		}
		current = *head;
	}
}

void FinalizeTablesThreadTriple(uint64_t** Index1, uint64_t** Index2, bucket **head, int columns){
	// Sort the input tables
	bucket *current = *head;
	int hist[256];
	uint64_t psum[256], **From, **To;

	while(current!=NULL){

		if(current->flag % 2 == 1){
			From = Index1;
			To = Index2;
		}
		else{
			From = Index2;
			To = Index1;
		}

		if(((current->end - current->start)*sizeof(From[0][1]) > 64*1024) && (current->numofbyte < 7)){
			// This bucket is bigger than 64KB and need to be divided
			uint64_t from = current->start, to = current->end;
			int numofbyte = current->numofbyte + 1;

			// Create a hist based on this bucket (hashed on the next byte)
			CreateHistTriple(From, hist, from, to, numofbyte);

			//Create psum
			CreatePsum(hist, psum);

			// Divide the bucket to smaller ones
			DivideBucket(head, hist, psum, current, numofbyte);
						
			// Reorder this newly made part of the table						
			ReorderTriple(From, To, psum, from, to, numofbyte, columns);
		}
		else{
			if(current->numofbyte < 7)
				MyqsortTriple(From, (int64_t)current->start, (int64_t)current->end-1, columns);

			// It is ready to be copied to the other index table
			CopyBucketTriple(From, To, current->start, current->end, columns);

			RemoveBucket(head, current);
		}
		current = *head;
	}
}

void SortTriple(int64_t** indexA, uint64_t rowsA, int num_of_files,pthread_cond_t* thread_cond, pthread_mutex_t* thread_mut, int* waiting){
	// Sorts the given index table (index table has 3 columns)
	int hist[256],i;
	uint64_t psum[256];

	// Create hist and psum for Index A
	CreateHistTriple(indexA, hist, 0, rowsA, 0);
	CreatePsum(hist, psum);

	int64_t** twin;
	// Create its twin table
	twin = malloc( sizeof(uint64_t) * rowsA);
	for(i=0 ; i<rowsA ; i++)
		twin[i]=malloc( sizeof(int64_t) *(num_of_files+1));
	
	// Reorder Index A to twin table
	ReorderTriple(indexA, twin, psum, 0, rowsA, 0, num_of_files+1);

	// Create psum again because it was ruined from Reorder!
	CreatePsum(hist, psum);

	bucket *head = NULL;
	CreateBucketList(&head, hist, psum);

	// Sort table A
	FinalizeTablesTriple(indexA, twin, &head, num_of_files+1, thread_cond,thread_mut, waiting);

	for(i=0 ; i<rowsA ; i++)
		free(twin[i]);
	free(twin);
}
