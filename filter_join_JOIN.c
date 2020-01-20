#include "header.h"

extern pthread_mutex_t sjoblist_mut;
extern sem_t sthread;
extern Job* JobHead;

int Filter(uint64_t** array, uint64_t rows,uint64_t column, int key, int flag, Current_Table* cur_table,int arraynumber, pthread_cond_t* thread_cond, pthread_mutex_t* thread_mut, int* waiting){

	uint64_t** Index, i;
	int64_t counter=0;

    rows = CreateNewIndex(cur_table, column, array, rows, &Index,arraynumber, thread_cond, thread_mut, waiting);

    FilRowIds *head = NULL, *current = NULL;

	switch (flag){
   		case 2:
      		//smaller than
      		for(i=0 ; i<rows && Index[i][1] < key ; i++){
         		//This loop will continue until a key bigger than or equal with key is found, or when the index table is over
	    		current = InsertFilterId(&head, current, Index[i][0]);
	    		counter++;
    		}
    		break;
		case 1:
      		// greater than
         	for(i=0; i<rows && Index[i][1] <= key; i++){
         		//Go through all the keys smaller than or equal with key
				}
         	for( ; i<rows ; i++){
         		//Start from the point you just ended and create the id list until the index table is over
    			current = InsertFilterId(&head, current, Index[i][0]);
    			counter++;
    		}
         	break;
		case 0:
      		// equal
         	for(i=0; i<rows && Index[i][1] != key; i++){
         		//Go through all the keys smaller than key
         	}
         	for( ; i<rows && Index[i][1] == key ; i++){
         		//Create the id list until the index table is over or a bigger key is found
	    		current = InsertFilterId(&head, current, Index[i][0]);
	    		counter++;
	    	}
         	break;
     	default:
         	printf("Something went wrong with the filter flag\n");
      		return -1;
    }
	FreeIndex(Index, rows);
	
	// If no row has been kept, the query ends here
	if(counter == 0)
   		return -1;

	//Delete previous filtered list if it existed and update with new one
	DeleteCurFilList(&(cur_table->filtered));
	cur_table->filtered = head;
	cur_table->rows = counter;
   	cur_table->column = column;

   	return 0;
}

int SelfJoin(RowIds** Result, uint64_t** array,uint64_t rows, uint64_t column1, uint64_t column2, Current_Table* cur_table,int arraynumber,int num_of_files, pthread_cond_t* thread_cond, pthread_mutex_t* thread_mut, int* waiting){

	uint64_t **Index, i;
	int64_t counter = 0;
    
   	FilRowIds *head = NULL, *cur = NULL;
	

	if(cur_table->column == -2){
		// If this table has not been proccessed
		
		//Create index for the column1
		rows= CreateNewIndex(cur_table, column1, array, rows, &Index, arraynumber, thread_cond, thread_mut, waiting);
    
   		for(i=0; i<rows; i++)
   			if(Index[i][1] ==  array[column2][Index[i][0]]){
				cur = InsertFilterId(&head, cur, Index[i][0]);
				counter++;
			}
		
	      	cur_table->rows = counter;
   		   	cur_table->column = column1; //or column 2
   		   	cur_table->filtered = head;
      
    	  	FreeIndex(Index,rows);
	 		// If this self join has not given any rows the query ends here
    	  	if(counter == 0)
				return -1;
	}
	else if(cur_table->column > -2){
		// This table has been in the filtered section
		FilRowIds *current = cur_table->filtered, *prev = current;
			
		for(i=0; current!=NULL ; i++){
   	
   			if(array[column1][current->id] != array[column2][current->id]){
				current = DeleteFilterId(&cur_table->filtered, current, prev);
				counter++;
			}
			else{
				prev = current;
				current = current->next;
			}
		}		
		// If it returns 0 rows the query ends here
		if(counter == cur_table->rows)
			return -1;

		cur_table->rows = cur_table->rows - counter;
		// curtable's column stays the same because we only deleted tuples
	}
	return 0;
}

int64_t EasyJoin(RowIds** Result,uint64_t** array1,uint64_t** array2,uint64_t column1,uint64_t column2, int arraynumber1,int arraynumber2,int num_of_files){

	// The tables are already in the tuple list
	RowIds* current = *Result, *prev = current;
	uint64_t i;
	int64_t counter = 0;
	
	for(i=0; current!=NULL ; i++){
  		if(array1[column1][current->tuple[arraynumber1]] !=  array2[column2][current->tuple[arraynumber2]]){
			current = DeleteId(Result, current, prev);
			counter++;
		}
		else{
			prev=current;
			current = current->next;
		}
	}
	return counter;
}


uint64_t JoinList(Record** Head,uint64_t ** indexA,uint64_t ** indexB,uint64_t rowsA,uint64_t rowsB, int arraynumber1,int arraynumber2,int num_of_files, RowIds** Result, pthread_cond_t* thread_cond, pthread_mutex_t* thread_mut, int* waiting, Merge** head){
	
	int counter = 0, flagie = 0;
	*waiting = 3;
	uint64_t k,y,j,newstart = 0, startb = 0, endb = 0, end;

	for(int i=0 ; i<3 ; i++){
		Sjob *job = (Sjob*)malloc(sizeof(Sjob));
		
		job->From = indexA;
		job->To = indexB;
		job->flag = 3; // Join type
		job->start = counter;
		if(i < 2)
			counter += rowsA / 3;
		else
			counter = rowsA;
		job->end = counter;

		job->rowsB = rowsB;
		job->arraynumber1 = arraynumber1;
		job->arraynumber2 = arraynumber2;
		job->head = head;
		job->key = i;
		job->columns = num_of_files; //
		job->thread_cond = thread_cond;
		job->thread_mut = thread_mut;
		job->waiting = waiting;
		job->next = NULL;

		pthread_mutex_lock(&sjoblist_mut);
			SAddJob(JobHead, job);
		pthread_mutex_unlock(&sjoblist_mut);

		sem_post(&sthread);
	}

	//Wait until join is done
	pthread_mutex_lock(thread_mut);
		while(*waiting != 0){
			pthread_cond_wait(thread_cond,thread_mut);
		}
	pthread_mutex_unlock(thread_mut);

	if(*head == NULL)
		return 0;

	*Result = (*head)->head;
	Merge *prev = (*head), *temp = (*head)->next;

	RowIds *cur;
	uint64_t total_rows = prev->rows;

	while(temp != NULL){
		cur = prev->head;
		while(cur->next != NULL){
			cur = cur->next;
		}
		cur->next = temp->head;
		total_rows += temp->rows;
		temp = temp->next;
		prev = prev->next;
	}

	DeleteMergeList(head);
	return total_rows;
}

uint64_t JoinListThread(RowIds** Result, uint64_t ** indexA,uint64_t ** indexB, uint64_t start, uint64_t end, uint64_t rowsB, int arraynumber1,int arraynumber2,int num_of_files){
	
	uint64_t i, j, counter = 0, records = 0,sum=0;
	RowIds* current = NULL;
	
	for(i=start ; i<end ; i++){

		for(j=counter ; j<rowsB ; j++){
			
			if(indexA[i][1] == indexB[j][1]){
				//Insert the record
				records++;
				sum++;
				current = InsertId(Result, current, indexA[i][0], indexB[j][0], arraynumber1, arraynumber2, num_of_files);
			}
			else if(indexA[i][1] < indexB[j][1])
				break;
		}
		if(i < end-1 && indexA[i][1] == indexA[i+1][1])
			counter = j-records;
		else
			counter = j;
	
		records=0;
	}
	return sum;
}

int Synchronize(RowIds** Result, uint64_t ***Index, uint64_t rows, Current_Table* cur_table, uint64_t** array, int arraynumber1, int arraynumber2,uint64_t column, int num_of_files){
	// Synchronize the tuple list with a new table index
	RowIds* cur_id = *Result,*prev_id = cur_id;
	uint64_t cur_rec = 0, temp_rec = 0;

	while(cur_rec < rows && cur_id != NULL){

		if(array[column][cur_id->tuple[arraynumber1]] < (*Index)[cur_rec][1]){
			// If the key of the tuple list is smaller than the key of the index delete the tuple
			cur_id = DeleteId(Result, cur_id, prev_id);
			cur_table->rows--;
		}
		else if(array[column][cur_id->tuple[arraynumber1]] == (*Index)[cur_rec][1]){
			// If the keys are equal we have a join
			temp_rec=cur_rec;
				
			cur_id->tuple[arraynumber2] = (*Index)[cur_rec][0];
			cur_rec ++;
				
			while(cur_rec < rows && array[column][cur_id->tuple[arraynumber1]] == (*Index)[cur_rec][1]){
					
				cur_id = InsertId2(Result, cur_id, (*Index)[cur_rec][0], arraynumber2, num_of_files, &prev_id);

				cur_table->rows++;
				cur_rec ++;
			}
			cur_rec = temp_rec;
			prev_id = cur_id;
			cur_id = cur_id->next;
		}
		else if(array[column][cur_id->tuple[arraynumber1]] > (*Index)[cur_rec][1]){
			// If the key of the tuple list is greater than the key of the index continue on the index
				
			while(cur_rec < rows && array[column][cur_id->tuple[arraynumber1]] > (*Index)[cur_rec][1])
				cur_rec ++;
		}
	}
		
	while(cur_id!=NULL){
		// Delete any left over tuples
		cur_id = DeleteId(Result, cur_id, prev_id);
		cur_table->rows--;
	}
	// If the tuples are 0, the query ends here
	if(cur_table->rows == 0)
		return -1;
	return 0;
}

uint64_t FindResult(RowIds* Result, uint64_t** array, uint64_t column, int arraynumber){
	uint64_t sum=0;
	RowIds* current=Result;
	
	if(current==NULL)
		return 0;
		
	while(current!=NULL){
		sum = sum + array[column][current->tuple[arraynumber]];	
		current=current->next;
	}
	return sum;
}

uint64_t IndexFromTuples(RowIds** Result, Current_Table* cur_table, int64_t ***Index, int position, uint64_t column, int num_of_files, uint64_t** array){
	// Create an index table from the tuple list
	RowIds* temp = *Result;
	int64_t i;
	uint64_t rows = cur_table->rows;
	int j;
	    
    (*Index) = (int64_t**)malloc(sizeof(int64_t*)*rows);
	for(i=0; i<rows ;i++)
		(*Index)[i] = (int64_t*)malloc(sizeof(int64_t)*(num_of_files + 1));

	for(i=0 ; i<rows ; i++){
		for(j=1 ; j<num_of_files+1 ; j++){
			(*Index)[i][j] = temp->tuple[j-1];
		}
		// In the first column our index will have the real keys that we want it to be sorted with
		(*Index)[i][0] = array[column][(*Index)[i][position+1]];
		temp = DeleteId(Result, temp, *Result);
	}
   	cur_table->rows = 0;
	*Result = NULL;
	return rows;
}

void TuplesFromIndex(RowIds** Result, Current_Table* cur_table, int64_t ***Index, int num_of_files, uint64_t rows){
	// Recreate the tuple list from the now sorted Index table
	RowIds* temp = NULL;
	uint64_t i;
	int j;
    
	for(i=0 ; i<rows ; i++){
		// Insert an empty Tuple
		temp = InsertIdSyn(Result, cur_table, temp, num_of_files);
		
		for(j=0 ; j<num_of_files ; j++){
			temp->tuple[j] = (*Index)[i][j+1];
		}
	}
	
	// We no longer need the index table
	for(i=0; i<rows ;i++)
		free((*Index)[i]);
	free((*Index));
}

void SyncRows(Current_Table** Cur_Head, int64_t newrows, int num_of_files){
	// Synchronize rows
	int64_t i;
	for(i=0 ;i < num_of_files ; i++)
		if((*Cur_Head)[i].column > -2 && (*Cur_Head)[i].filtered == NULL)
			(*Cur_Head)[i].rows = newrows;
}

void DestroyCols(Current_Table** Cur_Head, int num_of_files){

	int64_t i;
	for(i=0 ;i < num_of_files ; i++)
		if((*Cur_Head)[i].column > -2 && (*Cur_Head)[i].filtered == NULL)
			(*Cur_Head)[i].column = -1;
}

int Join(RowIds** Result, uint64_t** array1,uint64_t** array2,uint64_t rows1, uint64_t rows2,uint64_t column1,uint64_t column2,Current_Table** Cur_Head, Current_Table* cur_table1,Current_Table* cur_table2,int arraynumber1,int arraynumber2,int num_of_files,  pthread_cond_t* thread_cond, pthread_mutex_t* thread_mut, int* waiting, Merge** head){

	uint64_t records = 0;
	
	if(cur_table1->column > -2 && cur_table1->filtered == NULL && cur_table2->column > -2 && cur_table2->filtered == NULL){
		// If the tables already exist in the tuple list
		int counter = EasyJoin(Result, array1,array2,column1,column2,arraynumber1,arraynumber2,num_of_files);
		
		if((*Result) == NULL && counter==cur_table1->rows)
			return -1;

		SyncRows(Cur_Head, cur_table2->rows - counter, num_of_files);
		
		return 0;
	}
	else{
		if(cur_table1->column > -2 && cur_table1->filtered == NULL){
			// If table A is already joined in the tuple list and B is not
			uint64_t **Index2, rowsB;

			rowsB = CreateNewIndex(cur_table2, column2, array2, rows2, &Index2, arraynumber2, thread_cond, thread_mut, waiting);
			
			DeleteCurFilList(&(cur_table2->filtered));
			
			if(cur_table1->column == column1){
				// A Tuples are already sorted in the correct column
				if(Synchronize(Result, &Index2, rowsB, cur_table1, array1, arraynumber1, arraynumber2, column1, num_of_files) == -1){
					FreeIndex(Index2, rowsB);
					return -1;
				}
			
				cur_table2->column = column2;
				SyncRows(Cur_Head, cur_table1->rows, num_of_files);
			}
			else{
				// We have to sort the tuples in the correct column
				int64_t **Index;
				uint64_t rows;
				
				rows = IndexFromTuples(Result, cur_table1, &Index, arraynumber1, column1, num_of_files, array1);

				SortTriple(Index, rows, num_of_files,thread_cond, thread_mut, waiting);

				TuplesFromIndex(Result,cur_table1, &Index, num_of_files, rows);


				if(Synchronize(Result, &Index2, rowsB, cur_table1, array1, arraynumber1, arraynumber2, column1, num_of_files) == -1){
					FreeIndex(Index2, rowsB);
					return -1;
				}
				// The rest tuple list is not ordered in any column now
				DestroyCols(Cur_Head, num_of_files);

				cur_table1->column = column1;
				cur_table2->column = column2;

				SyncRows(Cur_Head, cur_table1->rows, num_of_files);
			}
			FreeIndex(Index2,rowsB);
		}
		else if(cur_table2->column > -2 && cur_table2->filtered == NULL){
			// If table B is already joined in the tuple list and A is not
			uint64_t **Index1, rowsA;

			rowsA = CreateNewIndex(cur_table1, column1, array1, rows1, &Index1, arraynumber1, thread_cond, thread_mut, waiting);

			DeleteCurFilList(&(cur_table1->filtered));
	
			if(cur_table2->column==column2){
				// B Tuples are already sorted in the correct column
				if(Synchronize(Result, &Index1, rowsA, cur_table2, array2, arraynumber2, arraynumber1, column2, num_of_files) == -1){
					FreeIndex(Index1,rowsA);
					return -1;
				}

				cur_table1->column = column1;
				SyncRows(Cur_Head, cur_table2->rows, num_of_files);
			}
			else{
				// We have to sort the tuples in the correct column
				int64_t **Index;
				uint64_t rows;

				rows = IndexFromTuples(Result, cur_table2, &Index, arraynumber2, column2, num_of_files, array2);

				SortTriple(Index, rows, num_of_files,thread_cond, thread_mut, waiting);

				TuplesFromIndex(Result,cur_table2, &Index, num_of_files, rows);
	
				if(Synchronize(Result, &Index1, rowsA, cur_table2, array2, arraynumber2, arraynumber1, column2, num_of_files) == -1){
					FreeIndex(Index1,rowsA);
					return -1;
				}

				// The rest tuple list is not ordered in any column now
				DestroyCols(Cur_Head, num_of_files);

				cur_table1->column = column1;
				cur_table2->column = column2;

				SyncRows(Cur_Head, cur_table2->rows, num_of_files);
			}
			FreeIndex(Index1,rowsA);
		}
		else{
			// If no table is on the tuple list, they are the first join
			uint64_t **Index1,**Index2;
			uint64_t rowsA,rowsB;

    		rowsA = CreateNewIndex(cur_table1, column1, array1, rows1, &Index1,arraynumber1, thread_cond, thread_mut, waiting);
    		rowsB = CreateNewIndex(cur_table2, column2, array2, rows2, &Index2,arraynumber2, thread_cond, thread_mut, waiting);	
		
			DeleteCurFilList(&(cur_table1->filtered));
			DeleteCurFilList(&(cur_table2->filtered));
			
			Record* Head = NULL, *cur_rec;
			RowIds* current = NULL;
			records = JoinList(&Head,Index1, Index2, rowsA,rowsB, arraynumber1, arraynumber2, num_of_files, Result, thread_cond, thread_mut, waiting, head);
			
			// If the tuples are 0 the query ends here
			if(records==0){
				FreeIndex(Index1,rowsA);
				FreeIndex(Index2,rowsB);
				return -1;
			}
			cur_table1->column = column1;
			cur_table2->column = column2;

			SyncRows(Cur_Head, records, num_of_files);
			
			FreeIndex(Index1,rowsA);
			FreeIndex(Index2,rowsB);
		}
	}
	return 0;
}

Solution* FindSolution(COMMANDLIST *command,uint64_t ***arrayname,Initial_Table* Table,Solution** Sol_Head,Solution* cur, pthread_cond_t* thread_cond, pthread_mutex_t* thread_mut, int* waiting,Statistics** initStats, Merge** head){
	// Find the solution of a query
	
	int* array, i, answer = 0, num_of_files = command->num_of_files;
	array = malloc(sizeof(int)*num_of_files);
	Arrays* point = command->arraylist;
	for(i=0; i<num_of_files; i++){
		array[i] = point->arrayname;
		point = point->next;
	}
	
	Statistics *stats[num_of_files];
	for(i=0 ; i<num_of_files ; i++)
		stats[i]=malloc(sizeof(Statistics)*Table[array[i]].columns);
	
	PROJECTION* pro;
	pro = command->projections->next;
	
	cur = malloc(sizeof(Solution));
        
	cur->sum = malloc(sizeof(uint64_t)*command->projectionCount);
	cur->next = NULL;
	cur->counter = command->projectionCount;
	
	CopyStats(initStats,stats,array,Table,num_of_files);
	answer = newSort(command, array,Table,initStats, stats);
	DeleteStats(stats,num_of_files);
	
	if(answer == -1){			// No result print NULL
		i=0;
	
		while(pro != NULL){
			// The projections of this query are NULL
			cur->sum[i]= 0;
			pro=pro->next;
			i++;
		}
		free(array);
		return cur;
	}
	
	Current_Table* Cur_Table = malloc(sizeof(Current_Table)*num_of_files);
	Initialize_Cur_Table(Cur_Table, num_of_files);
	
	RowIds* Result=NULL;
	
	NODE* query;
	query = command->nodes->next;
	while(query != NULL){
		// Read all the predicates
		
		if(query->secondArray==-1){
			// Filter
			answer=Filter(arrayname[array[query->firstArray]],Table[array[query->firstArray]].rows,query->firstCol,query->secondCol , query->commandType, &Cur_Table[query->firstArray],query->firstArray, thread_cond, thread_mut, waiting);
			if(answer==-1)
				// If there is no result stop with this query
				break;
		}
		else{
			// Join
			if(query->firstArray == query->secondArray){
				// Join between the same array- Self Join
				answer= SelfJoin(&Result,arrayname[array[query->firstArray]],Table[array[query->firstArray]].rows, query->firstCol,query->secondCol, &Cur_Table[query->firstArray],query->firstArray,num_of_files, thread_cond, thread_mut, waiting);
				if(answer==-1)
					break;
			
			}
			else{
				// Join between two different arrays
				answer= Join(&Result, arrayname[array[query->firstArray]], arrayname[array[query->secondArray]], Table[array[query->firstArray]].rows, Table[array[query->secondArray]].rows,query->firstCol,query->secondCol,&Cur_Table, &Cur_Table[query->firstArray], &Cur_Table[query->secondArray], query->firstArray,query->secondArray,num_of_files, thread_cond, thread_mut, waiting, head);
			
				if(answer==-1)
					break;		
			}		
		}
		query=query->next;
	}
	
	
	i=0;
	
	while(pro != NULL){
		// Calculate the projections of this query
		cur->sum[i]= FindResult(Result, arrayname[array[pro->array]], pro->col, pro->array);
		pro=pro->next;
		i++;
	}
	free(Cur_Table);
	free(array);
	FreeResult(&Result);

	return cur;
}
