#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "header.h"

/* PROJECTION is our structure that keeps data that should be projected in each node */

PROJECTION *projectionInit(PROJECTION *projectionHead){
	projectionHead = malloc(sizeof(PROJECTION));

	projectionHead->array = -1;
	projectionHead->col = -1;
	projectionHead->next = NULL;

	return projectionHead;
}

void projectionFree(PROJECTION *projectionHead){

	PROJECTION *temp;
	temp = projectionHead;
	while(projectionHead->next != NULL){
		temp = projectionHead->next;
		projectionHead->next = projectionHead->next->next;
		free(temp);
	}
}

void projectionInsert(PROJECTION *projectionHead, __uint64_t array, __uint64_t col){

	PROJECTION *temp = NULL;
	temp = projectionHead;

	while(temp->next != NULL){
		temp = temp->next;
	}

	temp->next = malloc(sizeof(PROJECTION));
	temp = temp->next;
	temp->array = array;
	temp->col = col;
	temp->next = NULL;
}

/* NODE is our structure that keeps work commands that should be executed in each node */
NODE *nodeInit(NODE *nodeHead){

	nodeHead = malloc(sizeof(NODE));
	nodeHead->firstArray = -1;
	nodeHead->firstCol = -1;
	nodeHead->secondArray = -1;
	nodeHead->secondCol = -1;	
	nodeHead->commandType = -1;
	nodeHead->next = NULL;

	return nodeHead;
}

void nodeFree(NODE *nodeHead){

	NODE *temp;
	temp = nodeHead;
	while(nodeHead->next != NULL){
		temp = nodeHead->next;
		nodeHead->next = nodeHead->next->next;
		free(temp);
	}
}

/* COMMANDLIST is our structure that keeps all the work-related (work commands, projections, arrays used) that should be used during our program */
COMMANDLIST *commandListInit(COMMANDLIST *commandListHead){

	NODE *nodeHead = NULL;

	commandListHead = malloc(sizeof(COMMANDLIST));

	nodeHead = nodeInit(nodeHead);
	commandListHead->nodes = nodeHead;

	PROJECTION *projectionHead = NULL;
	projectionHead = projectionInit(projectionHead);
	commandListHead->projections = projectionHead;

	commandListHead->arraylist=NULL;
	commandListHead->num_of_files=0;
	
	commandListHead->projectionCount = 0;
	commandListHead->lineCounter = 0;

	return commandListHead;
}

void nodeInsert(NODE *nodeHead, __uint64_t firstArray, __uint64_t firstCol, __uint64_t secondArray, __uint64_t secondCol, int commandType){

	NODE *temp = NULL;
	temp = nodeHead;
	
	while(temp->next != NULL){
		temp = temp->next;
	}

	temp->next = malloc(sizeof(NODE));
	temp = temp->next;
	temp->firstArray = firstArray;
	temp->firstCol = firstCol;
	temp->secondArray = secondArray;
	temp->secondCol = secondCol;
	temp->commandType = commandType;
	temp->next = NULL;
}


/* read command uses the lineCounter variable, to get to the next line that should be read from the work file, reads it and increments lineCounter by 1
	,next it breaks each command-part in tokens, and each token in data that eventually stores in our structures */ 
int readCommand(COMMANDLIST *commandListHead,int Flag){

	if(commandListHead == NULL){
		printf("Error: command list is NULL, returning . . .\n");
		return -1;
	}

	char *filePath;
	if(Flag==1)
		filePath= "/tmp/workloads/small/small.work";
	else if(Flag==2)
		filePath= "/tmp/workloads/medium/medium.work";
	else if(Flag==3)
		filePath= "small.work";

	char command[300];
	int counter = 0;

	FILE *file = fopen(filePath, "r");
	if(file == NULL)
		printf("ERROR: Could not open the file.\n");

	if(file == NULL){
		printf("Error: could not open the file. returning . . .\n");
		return -1;
	}else{

		while(fgets(command, sizeof(command), file) != NULL){

			if(counter == (commandListHead)->lineCounter){
				/*we increment the lineCounter, so that next time we are calling this function,
				it will return the next line */
				(commandListHead)->lineCounter+=1;
				/*reading lines until we reach the one that should be read next*/
				commandListHead->projectionCount = 0;
				break;
			}else{
				counter+=1;
			}
		}

		if(command[0] == 'F'){
			
			fclose(file);
			return 0;
		}

		char *copy_command,*token;
		char* rest;
		char array[3];
		
		copy_command=malloc(sizeof(char)*(strlen(command)+1));
		strcpy(copy_command,command);
		
		/*commands first part, reading the arrays that will be used in our query */
		
		token=strtok(copy_command,"|");
		rest=token;
		Arrays* cur=NULL;
		
		while((token=strtok_r(rest," ",&rest))) {
			cur=InsertArraylist(&(commandListHead->arraylist),cur,atoi(token));
			commandListHead->num_of_files++;
		}
		
		free(copy_command);
		/* breaking the command string into tokens */

		char *tokenString = malloc(sizeof(char)*301);
		memset(tokenString, 0, 301);

		char *tokenInfo = malloc(sizeof(char)*301);
		memset(tokenInfo, 0, 301);
		
		counter = 0;
    	
		int i = 0;

    	while(i<strlen(command)){
    		if(command[i] == '|'){
    			i+=1;
    			counter+=1;
    		
    		}
    		/*commands second part, reading the query data that will be used 
    		if the query is a filter, then the secondArray command will have the -1 value */
    		if(counter == 1){

			    __uint64_t firstArray;
			    __uint64_t firstCol;
			    __uint64_t secondArray;
			    __uint64_t secondCol;
			    int commandType;

		    	while(command[i] != '|'){

		    		if(command[i] == '&'){

		    			firstArray = -1;
		    			firstCol = -1;
		    			secondArray = -1;
		    			secondCol = -1;
		    			commandType = -1;

			    		for(int j=0;j<strlen(tokenString);j++){
			    			
			    			if(tokenString[j] == '.'){

			    				if(firstArray != -1){
			    					secondArray = atoi(tokenInfo);
			    				}else{
			    					firstArray = atoi(tokenInfo);
			    				}

			    				memset(tokenInfo, 0, strlen(tokenInfo));

			    			}else if(tokenString[j] == '='){
			    							    				
			    				if(firstCol != -1){
			    					secondCol = atoi(tokenInfo);
			    				}else{
			    					firstCol = atoi(tokenInfo);
			    				}

			    				commandType = 0;

			    				memset(tokenInfo, 0, strlen(tokenInfo));

			    			}else if(tokenString[j] == '>'){

			    				if(firstCol != -1){
			    					secondCol = atoi(tokenInfo);
			    				}else{
			    					firstCol = atoi(tokenInfo);
			    				}

			    				commandType = 1;

			    				memset(tokenInfo, 0, strlen(tokenInfo));

			    			}else if(tokenString[j] == '<'){

			    				if(firstCol != -1){
			    					secondCol = atoi(tokenInfo);
			    				}else{
			    					firstCol = atoi(tokenInfo);
			    				}

			    				commandType = 2;

			    				memset(tokenInfo, 0, strlen(tokenInfo));
			    			}else{
				    			tokenInfo[strlen(tokenInfo)] = tokenString[j];	
			    			}

			    		}

			    		if(firstCol != -1){
			    			secondCol = atoi(tokenInfo);
			    		}else{
			    			firstCol = atoi(tokenInfo);
			    		}

			    		nodeInsert((commandListHead)->nodes, firstArray, firstCol, secondArray, secondCol, commandType);

						memset(tokenInfo, 0, strlen(tokenInfo));			    		
			    		memset(tokenString, 0, strlen(tokenString));

		    		}else{
		    			tokenString[strlen(tokenString)] = command[i];
		    		}
		    		i+=1;
	    		}
    		
    			/*last node created from token info (command second part last node)*/
		    	firstArray = -1;
		    	firstCol = -1;
		    	secondArray = -1;
		    	secondCol = -1;
		    	commandType = -1;

			    for(int j=0;j<strlen(tokenString);j++){
					
			    	if(tokenString[j] == '.'){

	    				if(firstArray != -1){
			    			secondArray = atoi(tokenInfo);
			    		}else{
							firstArray = atoi(tokenInfo);
	    				}

			    		memset(tokenInfo, 0, strlen(tokenInfo));

			    	}else if(tokenString[j] == '='){

			    				
			    		if(firstCol != -1){
			    			secondCol = atoi(tokenInfo);
			    		}else{
			    			firstCol = atoi(tokenInfo);
			    		}
			    		commandType = 0;

			    		memset(tokenInfo, 0, strlen(tokenInfo));

			    	}else if(tokenString[j] == '>'){

			    		if(firstCol != -1){
			    			secondCol = atoi(tokenInfo);
			    		}else{
			    			firstCol = atoi(tokenInfo);
			    		}
			    		commandType = 1;

			    		memset(tokenInfo, 0, strlen(tokenInfo));

			    	}else if(tokenString[j] == '<'){

			    		if(firstCol != -1){
			    			secondCol = atoi(tokenInfo);
			    		}else{
			    			firstCol = atoi(tokenInfo);
			    		}
			    		commandType = 2;

			    		memset(tokenInfo, 0, strlen(tokenInfo));
			    	}else{
				    	tokenInfo[strlen(tokenInfo)] = tokenString[j];	
			    	}

			    }
			    if(firstCol != -1){
			    	secondCol = atoi(tokenInfo);
			    }else{
			    	firstCol = atoi(tokenInfo);
			    }
			    nodeInsert((commandListHead)->nodes, firstArray, firstCol, secondArray, secondCol, commandType);

				memset(tokenInfo, 0, strlen(tokenInfo));			    		
			    		
				memset(tokenString, 0, strlen(tokenString));	    			
	
	    		i+=1;
	    		counter+=1;
			}
			/* commands third part, reading all the projections in the PROJECTION structure of our list */
			if(counter == 2){

				memset(tokenInfo, 0, strlen(tokenInfo));
				
				__uint64_t array = 0;
				__uint64_t col = 0;

				while(i < strlen(command)){
				
					if(command[i] == '.'){
					
						array = atoi(tokenInfo);
						memset(tokenInfo, 0, strlen(tokenInfo));
					}else if(command[i] == ' '){
					
						col = atoi(tokenInfo);
						projectionInsert(commandListHead->projections, array, col);
						commandListHead->projectionCount+=1;
						memset(tokenInfo, 0, strlen(tokenInfo));
					}else{
						tokenInfo[strlen(tokenInfo)] = command[i];
					}
					i+=1;
				}
			
				col = atoi(tokenInfo);
				projectionInsert(commandListHead->projections, array, col);
				commandListHead->projectionCount+=1;
				memset(tokenInfo, 0, strlen(tokenInfo));
			}
			i+=1;
    	}
    	free(tokenInfo);
    	free(tokenString);
	}
	fclose(file);

	commandListSort(commandListHead);
	return 1;
}

/* function that swaps nodes */
void swapWorkNodes(NODE *node1, NODE *node2){

	NODE *temp = malloc(sizeof(NODE));
	temp->firstArray = node1->firstArray;
	temp->firstCol = node1->firstCol;
	temp->secondArray = node1->secondArray;
	temp->secondCol = node1->secondCol;
	temp->commandType = node1->commandType;

	node1->firstArray = node2->firstArray;
	node1->firstCol = node2->firstCol;
	node1->secondArray = node2->secondArray;
	node1->secondCol = node2->secondCol;
	node1->commandType = node2->commandType;

	node2->firstArray = temp->firstArray;
	node2->firstCol = temp->firstCol;
	node2->secondArray = temp->secondArray;
	node2->secondCol = temp->secondCol;
	node2->commandType = temp->commandType;

	free(temp);
}

void commandListSort(COMMANDLIST *commandListHead){

	NODE *temp = commandListHead->nodes;
	NODE *cur = temp;
	NODE *search = temp;

	if(temp->next != NULL){
		temp = temp->next;
		cur = temp;
		search = temp;
	}else{
		printf("Work list is empty, nothing to sort. \n returning . . .");
		return;
	}

	/*************** sorting filters ************/

	/*if node is filter, dont change its priority and go for next */
	while(cur->secondArray == -1 && cur->next != NULL){
		cur = cur->next;
	}

	search = cur;

	/*while search finds a filter node, swap it with the 'cur' one, and the cur = cur-next, so that we have the 
	next position for swap, this will bring all the filter nodes to the front of our list */

	while(search != NULL){

		if(search->secondArray == -1){
			if(search && cur){
				swapWorkNodes(search, cur);
				cur = cur->next;
			}
		}

		search = search->next;

	}
	/****************sorting filters ends****************/

	temp = cur;
	search = cur;

	/**************** sorting self joins ****************/

	/*if node is self-join , dont change its priority and go for next */
	while(cur->firstArray == cur->secondArray && cur->next != NULL){
		cur = cur->next;
	}
	search = cur;

	/*while search finds a self-join node, swap it with the 'cur' one, and the cur = cur-next, so that we have the next 
	position for swap, this will bring all the self-join nodes to the front of our list, after the filter ones 
	(because the cur is past the filters) */

	while(search != NULL){

		if(search->firstArray == search->secondArray){
			if(search && cur){
				swapWorkNodes(search, cur);
				cur = cur->next;
			}
		}
		search = search->next;
	}
	temp = cur;
	cur = cur->next;
	search = cur;

	/**************** sorting self joins ends ****************/
	
}

void commandListFree(COMMANDLIST *commandListHead){

	nodeFree(commandListHead->nodes);
	projectionFree(commandListHead->projections);
	free(commandListHead->nodes);
	free(commandListHead->projections);
	free(commandListHead);	
}
