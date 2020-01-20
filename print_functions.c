// HELPFUL PRINTING FUNCTIONS

#include "header.h"

void PrintTable(uint64_t** Table, uint64_t rows){
	uint64_t i;

	for(i=0; i<rows; i++)
		printf("%lu | %lu\n", Table[i][0], Table[i][1]);
}

void PrintBucketNode(bucket* node){
	printf("From %lu to %lu\n", node->start, node->end);
}

void PrintHist(int* hist){
	int i;

	for(i=0; i<256; i++)
		if(hist[i] != 0)
			printf("%d %d\n", i, hist[i]);
}

void PrintList(Record *head){
	while(head != NULL){
		printf("rowidA: %lu \t rowIdB: %lu\n", head->rowIdA, head->rowIdB);
		head = head->next;
	}
	printf("\n");
}

void PrintRows(RowIds *head,int num_of_files){
	int i;	
	
	if(head==NULL){
		printf("Empty\n");
		return;	
	}
	while(head != NULL){
		for(i=0; i<num_of_files ; i++)
			printf("%ld\t", head->tuple[i]);
		printf("\n");
		head = head->next;
	}
}

void PrintFilRows(FilRowIds *head,uint64_t **array, uint64_t column){
	int i;	
	
	if(head==NULL){
		printf("Empty\n");
		return;	
	}
	while(head != NULL){
		printf("row: %ld\t key: %lu", head->id, array[column][head->id]);
		printf("\n");
		head = head->next;
	}
}

void projectionPrint(PROJECTION *projectionHead){

	int i = 0;

	PROJECTION *temp;
	temp = projectionHead;

	printf("Printing projections . . .\n");

	while(temp->next != NULL){
		temp = temp->next;
		printf("projection:[%d] with\t%ld %ld\t\n",i, temp->array, temp->col);
		i+=1;
	}


}

void PrintSolution(Solution* Head){
	
	if(Head==NULL)
		printf("Empty\n");
	while(Head!=NULL){
		for(int i=0; i< Head->counter ; i++){
			if( Head->sum[i]==0)
				printf("NULL ");
			else
				printf("%lu ", Head->sum[i]);
		}
		
		printf("\n");
		Head=Head->next;	
	}

}

void PrintArraylist(Arrays* Head){

	if(Head==NULL)
		printf("Empty\n");
	while(Head!=NULL){
		printf("\t\t\t\t%d\n", Head->arrayname);
		Head=Head->next;	
	}

}

void nodePrint(NODE *nodeHead){

	int i = 0;

	NODE *temp;
	temp = nodeHead;

	printf("Printing command nodes . . .\n");

	while(temp->next != NULL){
		temp = temp->next;
		printf("node:[%d] with\t%ld %ld\t%ld %ld\tcomType %d\n",i, temp->firstArray, temp->firstCol, temp->secondArray, temp->secondCol, temp->commandType);
		i+=1;
	}


}
