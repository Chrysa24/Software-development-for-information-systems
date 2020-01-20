#include "header.h"

//Global variables

extern pthread_cond_t batch_cond;
  
extern pthread_mutex_t batch_mut ;
extern pthread_mutex_t joblist_mut;
extern pthread_mutex_t sjoblist_mut;
extern pthread_mutex_t sollist_mut;
extern pthread_mutex_t merge;

extern sem_t qthread, sthread;
extern int flag_t;
extern int batch_counter;

extern Job* JobHead; // Head of the Jobs List
extern Solution* Sol_List;

void InsertSolList(Solution** head, Solution *node){
	if(*head == NULL){
		*head = node;
	}
	else if((*head)->key > node->key){
		node->next = (*head);
		*head = node;
	}
	else{
		Solution *temp = *head;
		while(temp->next != NULL && temp->next->key < node->key)
			temp = temp->next;
		node->next = temp->next;
		temp->next = node;
	}
}

void WorkingOnQuery(Qjob* pointer, pthread_cond_t* thread_cond, pthread_mutex_t* thread_mut, int* waiting,Statistics** initStats, Merge** head){

	Solution* result;
	result = FindSolution(pointer->commandListHead, pointer->arrayname, pointer->Table, NULL, NULL, thread_cond, thread_mut, waiting,initStats, head);
	result->key = pointer->count;

	// Add on the solution list
	pthread_mutex_lock(&sollist_mut);
		InsertSolList(&Sol_List, result);
	pthread_mutex_unlock(&sollist_mut);

	pthread_mutex_lock(&batch_mut);
		batch_counter -- ;
		pthread_cond_signal(&batch_cond);
	pthread_mutex_unlock(&batch_mut);

	DeleteArraylist(&(pointer->commandListHead->arraylist));
	commandListFree(pointer->commandListHead);
	free(pointer);
}

Qjob* PopJob(Job* JobHead){
	Qjob* temp = JobHead->qjobs;
	JobHead->qjobs = JobHead->qjobs->next;
	return temp;
}

Sjob* SPopJob(Job* JobHead){
	Sjob* temp = JobHead->sjobs;
	if(JobHead->sjobs != NULL)
		JobHead->sjobs = JobHead->sjobs->next;
	else
		printf("ERROR: No available job.\n");
	
	return temp;
}

void AddMerge(Merge** head, Merge* myhead, uint64_t rows, int key){
	
	if(*head == NULL){
		*head = (Merge*)malloc(sizeof(Merge));
		(*head)->head = myhead;
		(*head)->key = key;
		(*head)->rows = rows;
		(*head)->next = NULL;
	}
	else if((*head)->key > key){
		Merge* node = (Merge*)malloc(sizeof(Merge));
		node->head = myhead;
		node->key = key;
		node->rows = rows;
		node->next = (*head);
		*head = node;
	}
	else{
		Merge *temp = *head;
		while(temp->next != NULL && temp->next->key < key)
			temp = temp->next;

		Merge* node = (Merge*)malloc(sizeof(Merge));
		node->head = myhead;
		node->key = key;
		node->rows = rows;
		node->next = temp->next;

		temp->next = node;
	}
}

void DeleteMergeList(Merge **head){
	Merge* next = *head, *cur;
	while(next != NULL){
		cur = next;
		next = next->next;
		free(cur);
	}
	*head = NULL;
}

int BucketsToJobs(bucket* current, Job *head, uint64_t** Index1, uint64_t** Index2, pthread_cond_t* thread_cond, pthread_mutex_t* thread_mut, int* waiting, int num_of_buckets){
	// Divide the given buckets to 3 sort jobs
	bucket* temp = current, *newhead, *last;
	int counter = 0;

	while(temp != NULL){
		Sjob *job = (Sjob*)malloc(sizeof(Sjob));
		
		job->From = Index1;
		job->To = Index2;
		job->flag = 1; // Sort Type 1
		job->columns = -1;
		job->mybucket = temp;
		job->thread_cond = thread_cond;
		job->thread_mut = thread_mut;
		job->waiting = waiting;
		job->next = NULL;

		if(counter < 2){
			for(int i=0 ; i< num_of_buckets/3-1 ; i++)
				temp = temp->next;
		
			last = temp;
			temp = temp->next;
			last->next = NULL;
		}
		else{
			SAddJob(head, job);
			counter++;;
			sem_post(&sthread);
			break;
		}

		SAddJob(head, job);
		counter ++;
		sem_post(&sthread);
	}
	return counter;
}

int BucketsToJobsTriple(bucket* current, Job *head, uint64_t** Index1, uint64_t** Index2, int columns, pthread_cond_t* thread_cond, pthread_mutex_t* thread_mut, int* waiting, int num_of_buckets){
	// Divide the given buckets to 3 sort jobs
	bucket* temp = current, *newhead, *last;
	int counter = 0;

	while(temp != NULL){
		Sjob *job = (Sjob*)malloc(sizeof(Sjob));
		
		job->From = Index1;
		job->To = Index2;
		job->columns = columns;
		job->flag = 2; // Sort Type 1
		job->mybucket = temp;
		job->thread_cond = thread_cond;
		job->thread_mut = thread_mut;
		job->waiting = waiting;
		job->next = NULL;

		if(counter < 2){
			for(int i=0 ; i< num_of_buckets/3-1 ; i++)
				temp = temp->next;
		
			last = temp;
			temp = temp->next;
			last->next = NULL;
		}
		else{
			SAddJob(head, job);
			counter++;;
			sem_post(&sthread);
			break;
		}

		SAddJob(head, job);
		counter ++;
		sem_post(&sthread);
	}
	return counter;
}

void *QueryThread(void** x){
	
	Statistics** initStats;
	initStats=(Statistics**)x;

	pthread_cond_t* thread_cond;
	thread_cond = (pthread_cond_t*) malloc(sizeof(pthread_cond_t));
	pthread_cond_init(thread_cond, NULL);
  
	pthread_mutex_t* thread_mut;
	thread_mut = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t));
	pthread_mutex_init(thread_mut, NULL);

	int* waiting;
	waiting = (int*)malloc(sizeof(int));

	Merge** head = (Merge**)malloc(sizeof(Merge*));
	*head = NULL;

	while(1){
		sem_wait(&qthread);
		
		if(flag_t == 1)
			break;

		pthread_mutex_lock(&joblist_mut);
			Qjob* current_job = PopJob(JobHead);
		pthread_mutex_unlock(&joblist_mut);

		if(current_job == NULL){
			printf("Somebody woke me up for no reason.\n");
			continue;
		}
		WorkingOnQuery(current_job, thread_cond, thread_mut, waiting, initStats, head);
	}
}

void *SortThread(void *vargp){

	while(1){
		sem_wait(&sthread);
		
		if(flag_t == 1)
			return;

		pthread_mutex_lock(&sjoblist_mut);
			Sjob* current_job = SPopJob(JobHead);
		pthread_mutex_unlock(&sjoblist_mut);

		if(current_job == NULL){
			printf("Somebody woke me up for no reason.\n");
			continue;
		}

		if(current_job->flag==1)
			FinalizeTablesThread(current_job->From, current_job->To, &current_job->mybucket);
		else if(current_job->flag==2)
			FinalizeTablesThreadTriple(current_job->From, current_job->To, &current_job->mybucket, current_job->columns);
		else if(current_job->flag == 3){
			RowIds * head = NULL;
			uint64_t rows;
			
			rows = JoinListThread(&head, current_job->From, current_job->To, current_job->start, current_job->end, current_job->rowsB, current_job->arraynumber1, current_job->arraynumber2, current_job->columns);

			if(rows > 0){
				pthread_mutex_lock(&merge);
					AddMerge(current_job->head, head, rows,current_job->key);
				pthread_mutex_unlock(&merge);
			}
		
		}
			
	
		pthread_mutex_lock(current_job->thread_mut);
			*(current_job->waiting) = *(current_job->waiting)-1;
			pthread_cond_signal(current_job->thread_cond);
		pthread_mutex_unlock(current_job->thread_mut);
	}
}

void InitializeJobsHead(Job **head){
	*head = (Job*)malloc(sizeof(Job));
	(*head)->qjobs = NULL;
	(*head)->sjobs = NULL;
}

void AddJob(Job *head, Qjob *node){
	if(head->qjobs == NULL)
		head->qjobs = node;
	else{
		node->next = head->qjobs;
		head->qjobs = node;
	}
}

void SAddJob(Job *head, Sjob *node){

	if(head->sjobs == NULL)
		head->sjobs = node;
	else{
		node->next = head->sjobs;
		head->sjobs = node;
	}
}
