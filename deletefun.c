#include "header.h"

void FreeIndex(uint64_t **Index,uint64_t rows){
	uint64_t i;
	for(i=0;i<rows; i++)
		free(Index[i]);
		
	free(Index);
}

RowIds* DeleteId(RowIds** head,RowIds* current,RowIds* prev){

	if((*head) == current){
		(*head) = current->next;
		free(current->tuple);
		free(current);	
		current = (*head);
		return current;
	}
	else{
		prev->next = current->next;
		free(current->tuple);
		free(current);
		current = prev->next;	
		return current;
	}
}

FilRowIds* DeleteFilterId(FilRowIds** head,FilRowIds* current,FilRowIds* prev){
	
	if((*head) == current){
		(*head) = current->next;
		free(current);
		current = (*head);
	}
	else{
		prev->next = current->next;
		free(current);
		current = prev->next;	
	}
	return current;
}

void DeleteCurFilList(FilRowIds** pointer){
    FilRowIds* cur = *pointer;
    while(*pointer != NULL){
        *pointer = (*pointer)->next;
        free(cur);
        cur = *pointer;
    }
}

void DeleteJoinList(Record** Head){
	Record* cur = *Head,*prev = cur;
	
	while(cur != NULL){
		prev = cur->next;
		free(cur);
		cur = prev;
	}
	(*Head) = NULL;
}

void DeleteSolution(Solution** Head){
	Solution* cur=*Head,*prev=*Head;
	while(cur!=NULL){
		prev=cur->next;
		free(cur->sum);
		free(cur);
		cur=prev;	
	}
	(*Head)=NULL;

}

void DeleteArraylist(Arrays** Head){
	Arrays* cur=*Head,*prev=*Head;
	while(cur!=NULL){
		prev=cur->next;
		free(cur);
		cur=prev;	
	}
	(*Head)=NULL;

}

void FreeResult(RowIds** Result){
	
	RowIds* prev=*Result, *cur=prev;
	while((cur)!=NULL){
		free(cur->tuple);
		prev=cur->next;
		free(cur);
		cur=prev;
	}
	
	(*Result)=NULL;	
}

