#include "header.h"

uint64_t CreateNewIndex(Current_Table* cur_table, uint64_t column, uint64_t** array, uint64_t rows, uint64_t ***Index, int arraynumber,  pthread_cond_t* thread_cond, pthread_mutex_t* thread_mut, int* waiting){
	//Created new index checking if there is a pre-existing one and return its rows
   	uint64_t i;

	if(cur_table->filtered != NULL){
   		// This table has been filtered
      	(*Index) = (uint64_t**)malloc(sizeof(uint64_t*)*(*cur_table).rows);
		for(i=0; i<(*cur_table).rows ;i++)
			(*Index)[i] = (uint64_t*)malloc(sizeof(uint64_t)*2);
	
		// Create Index Table
		FilRowIds* current = cur_table->filtered;
        
      	for(i=0; current != NULL ;i++){
      		//The index contains the pre-existing row ids and the keys from the wanted column
			(*Index)[i][0] = current->id;
			(*Index)[i][1] = array[column][current->id];
			
			current = current->next;
     	}
		if((*cur_table).column != column)
			Sort((*Index),(*cur_table).rows, thread_cond, thread_mut, waiting);

		return (*cur_table).rows;
	}
   	else if(cur_table->column == -2 && cur_table->filtered == NULL){
   		// There isn't any pre-existing index list in the current table so create one
    	(*Index) = (uint64_t**)malloc(sizeof(uint64_t*)*rows);
		for(i=0; i<rows ;i++)
			(*Index)[i] = (uint64_t*)malloc(sizeof(uint64_t)*2);
		
		CreateIndexTable((*Index), array, rows, column);
      	Sort((*Index),rows, thread_cond, thread_mut, waiting);

      	return rows;
   	}
   	else{
		printf("ERROR: CreateNewIndex\n");
		return -1;
   	}
}

void InitializeTuple(int64_t* tuple, int num){
	// Initialize a tuple with -1
	int i;
	for(i=0 ; i<num ; i++)
		tuple[i]=-1;
}

RowIds* InsertId2(RowIds** head, RowIds* current, uint64_t id, int arraynumber, int num_of_files, RowIds** prev){
	// Insert the new id after the prev node and before the current node
	RowIds* newnode;
	newnode = (RowIds*)malloc(sizeof(RowIds));
   	newnode->tuple = malloc(sizeof(int64_t)*num_of_files);

  	//InitializeTuple((newnode->tuple), num_of_files);
  	for(int i=0;i<num_of_files;i++)
  		newnode->tuple[i]=current->tuple[i];

   	newnode->tuple[arraynumber] = id;
   	newnode->next = current;
   
	if(current == (*head)){
   		*head = newnode;
		*prev = newnode;
      	return current;
	}
   	else{
   		(*prev)->next=newnode;
		*prev = newnode;
     	return current;
   	}
}

RowIds* InsertId(RowIds** head, RowIds* current, uint64_t id1,uint64_t id2, int arraynumber1,int arraynumber2,int num_of_files){
    //Current points to the last node, insert newnode at the end of the list

    if(current == NULL){
        current = (RowIds*)malloc(sizeof(RowIds));
        current->tuple =malloc(sizeof(int64_t)*num_of_files);

        InitializeTuple((current->tuple), num_of_files);
        current->tuple[arraynumber1] = id1;
        current->tuple[arraynumber2] = id2;
        current->next = NULL;
        
        *head = current;
        return current;
    }
    else{
        current->next = (RowIds*)malloc(sizeof(RowIds));
        current->next->tuple=malloc(sizeof(int64_t)*num_of_files);
        InitializeTuple((current->next->tuple), num_of_files);
        current->next->tuple[arraynumber1] = id1;
        current->next->tuple[arraynumber2] = id2;
        
        current->next->next = NULL;

        return current->next;
    }
}


FilRowIds* InsertFilterId(FilRowIds** head, FilRowIds* current, uint64_t id){
    //Current points to the last node, insert newnode at the end of the list

	if(current == NULL){
   	current = (FilRowIds*)malloc(sizeof(FilRowIds));

		current->id = id;
      	current->next = NULL;
        
      	*head = current;
      	return current;
    }
    else{
    	current->next = (FilRowIds*)malloc(sizeof(FilRowIds));
        
		current->next->id = id;
      	current->next->next = NULL;

      	return current->next;
    }
}

void Initialize_Cur_Table(Current_Table* Head, int num){
	int i;
	for(i=0;i<num ;i++){
		(Head[i]).rows = -1;
		(Head[i]).column = -2;
		(Head[i]).filtered = NULL;		
	}
}

void Insert_Result(Record** Head, uint64_t rowIdA, uint64_t rowIdB, Record** cur){

	Record *newnode = malloc(sizeof(Record));
	newnode->next = NULL;
	newnode->rowIdA = rowIdA;
	newnode->rowIdB = rowIdB;
		
	if((*Head)==NULL)
		(*Head)=newnode;
	else
		(*cur)->next = newnode;
	(*cur) = newnode;
}

RowIds* InsertIdSyn(RowIds** Result, Current_Table *cur_table, RowIds* current, int num_of_files){
    //Current points to the last node, insert newnode at the end of the list

    if(current == NULL){
		// This is the first node
        current = (RowIds*)malloc(sizeof(RowIds));
        current->tuple =malloc(sizeof(int64_t)*num_of_files);

        InitializeTuple((current->tuple), num_of_files);
        current->next = NULL;
        
        (*Result) = current;
        cur_table->rows = 1;
        return current;
    }
    else{
        current->next = (RowIds*)malloc(sizeof(RowIds));
        current->next->tuple = malloc(sizeof(int64_t)*num_of_files);
        InitializeTuple((current->next->tuple), num_of_files);
        
        current->next->next = NULL;

        cur_table->rows ++;
        return current->next;
    }
}


Solution* InsertSolution(Solution** Head, Solution* current, int num_of_res){

	Solution *temp = *Head;

	if(*Head == NULL){
		temp = malloc(sizeof(Solution));
        
      temp->sum = malloc(sizeof(uint64_t)*num_of_res);
      temp->next = NULL;
      temp->counter=num_of_res;
        
      *Head = temp;
	  return temp;
	}
	while(temp->next != NULL)
		temp = temp->next;
	temp->next = malloc(sizeof(Solution));
       
      temp->next->sum=malloc(sizeof(uint64_t)*num_of_res);
      temp->next->next = NULL;
      temp->next->counter=num_of_res;

      return temp->next;

	if(current == NULL){
		current = malloc(sizeof(Solution));
        
      current->sum = malloc(sizeof(uint64_t)*num_of_res);
      current->next = NULL;
      current->counter=num_of_res;
        
      *Head = current;
      return current;
    }
    else{
      current->next = malloc(sizeof(Solution));
       
      current->next->sum=malloc(sizeof(uint64_t)*num_of_res);
      current->next->next = NULL;
      current->next->counter=num_of_res;

      return current->next;
    }

}

Arrays* InsertArraylist(Arrays** Head,Arrays* current, int num){
	
	if(current == NULL){
		current = malloc(sizeof(Arrays));
        
      current->arrayname = num;
      current->next = NULL;
        
      *Head = current;
      return current;
    }
    else{
      current->next = malloc(sizeof(Arrays));
       
      current->next->arrayname=num;  
      current->next->next = NULL;

      return current->next;
    }

}
