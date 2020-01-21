#include <stdio.h>
//#include "work.h"
#include "header.h"
#include "statistics.h"
#include <stdbool.h>
#include <math.h>
#define N 49999999


ARRAYSTATISTICS* arrayStatsInit(ARRAYSTATISTICS *stats, int arrayCount, uint64_t ***arrayname, Initial_Table* Table){
	
	stats = malloc(sizeof(ARRAYSTATISTICS));
	stats->array = malloc(sizeof(ARRAYD)*arrayCount);
	stats->arrayCount = arrayCount;


	int min,max;
	uint64_t arrayRec;


	for(int y=0;y<arrayCount;y++){

		printf("\nCreating Statistics of array %d [%d] [%d] ###################\n", y, Table[y].columns, Table[y].rows);

		//malloc columns
		stats->array[y].colStats = malloc(sizeof(COLSTATISTICS)*Table[y].columns);
		stats->array[y].rows = Table[y].rows;
		stats->array[y].columns = Table[y].columns;


		for(int i=0;i<Table[y].columns;i++){

			max = arrayname[y][i][0];
			min = arrayname[y][i][0];

			for(int j=0;j<Table[y].rows;j++){

				arrayRec = arrayname[y][i][j];
				

				if(arrayRec < min){
					min = arrayRec;
				}else if(arrayRec > max){
					max = arrayRec;
				}

			}


			uint64_t la,ua,fa,da,range;
			la = min;
			ua = max;
			fa = Table[y].rows;
			da = 0;
			range = ua-la+1;
	

			//malloc rows
			if(range >= 50000000){
				range = N;
				
/*				printf("range is %ld \n", range);*/
				stats->array[y].colStats[i].arrayCheck = malloc(sizeof(bool)*range);
				memset(stats->array[y].colStats[i].arrayCheck, 0, sizeof(bool)*range);
	
				for(int j=0;j<Table[y].rows;j++){
					//printf("%d ", arrayname[arrayListTemp->arrayname][i][j]);
					arrayRec = arrayname[y][i][j];
					
					if(stats->array[y].colStats[i].arrayCheck[arrayRec-la%N] == false){
						da+=1;
						stats->array[y].colStats[i].arrayCheck[arrayRec-la%N] = true;
					}

	
	
				}
/*				printf("%ld unique recs \n", da);*/
			}else{

/*				printf("range is %ld \n", range);*/
				stats->array[y].colStats[i].arrayCheck = malloc(sizeof(bool)*range);
				memset(stats->array[y].colStats[i].arrayCheck, 0, sizeof(bool)*range);
	
				for(int j=0;j<Table[y].rows;j++){
					//printf("%d ", arrayname[arrayListTemp->arrayname][i][j]);
					arrayRec = arrayname[y][i][j];
					
					if(stats->array[y].colStats[i].arrayCheck[ua-arrayRec] == false){
						da+=1;
						stats->array[y].colStats[i].arrayCheck[ua-arrayRec] = true;
					}
					
				}
/*				printf("%ld unique recs \n", da);*/

			}
	

			stats->array[y].colStats[i].lA = la;
			stats->array[y].colStats[i].uA = ua;
			stats->array[y].colStats[i].fA = fa;
			stats->array[y].colStats[i].dA = da;
			//printf("%ld %ld %ld %ld \n", stats->array[y].colStats[i].lA,stats->array[y].colStats[i].uA,stats->array[y].colStats[i].dA,stats->array[y].colStats[i].fA);

		}

	}


	return stats;
}


ARRAYSTATISTICS* createStatistics(uint64_t ***arrayname, Initial_Table* Table, int arrayCount){

	printf("Hello from statistics \n");
	printf("\n");


	uint64_t max,min,arrayRec;


	ARRAYSTATISTICS *stats;


	stats = arrayStatsInit(stats, arrayCount, arrayname, Table);

	printf("\n");

	return stats;
}


void freeStatistics(ARRAYSTATISTICS *stats, int arrayCount, Initial_Table* Table){


	for(int y=0;y<arrayCount;y++){

		for(int i=0;i<Table[y].columns;i++){

			free(stats->array[y].colStats[i].arrayCheck);

		}

		free(stats->array[y].colStats);

	}
	free(stats->array);
	free(stats);



}


/*takes command list and stats, rearranges commands based on their cost to find optimal execution plan */
int commandListStatsCost(COMMANDLIST *commandListHead, ARRAYSTATISTICS *stats){

	//printf("Lets do some costs\n");

	NODE *temp = commandListHead->nodes;
	char com;
	int counter = 1;
//	printf("\nPrinting command list \n");


	ARRAYSTATISTICS *tempArray;
	tempArray = malloc(sizeof(ARRAYSTATISTICS));

	tempArray->arrayCount = stats->arrayCount;

	tempArray->array = malloc(sizeof(ARRAYD)*tempArray->arrayCount);

	memcpy(tempArray->array, stats->array, sizeof(ARRAYD)*tempArray->arrayCount);

	for(int i=0;i<tempArray->arrayCount;i++){

		tempArray->array[i].colStats = malloc(sizeof(COLSTATISTICS)*tempArray->array[i].columns);
		//printf("%d \n", tempArray->array[i].columns);
		memcpy(tempArray->array[i].colStats, stats->array[i].colStats, sizeof(COLSTATISTICS)*tempArray->array[i].columns);
		//printf("\n%ld\n",tempArray->array[i].colStats[0].fA);
		
		
	}


	NODE *newNodeList = NULL;
	newNodeList = nodeInit(newNodeList);
	NODE *tempNodeList = malloc(sizeof(NODE)*commandListHead->nodeCounter);
	NODE *tempNode = NULL;
	int i=0;
	int offsetNodeCount = 0; //counts how mant nodes we added to our new bestTree


	while(temp->next != NULL){
		temp = temp->next;
		memcpy(&tempNodeList[i], temp, sizeof(NODE));
		i++;
	}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	//nodeListSwap(&tempNodeList[0],&tempNodeList[1]);

	i=0;

	//this for loop updates statistics using all the filters
	for(int i=0;i<commandListHead->nodeCounter;i++){

		updateNodeCost(&tempNodeList[i], tempArray, commandListHead->arraylist);


		if(tempNodeList[i].secondArray == -1){
			
			//printf("adding filter to new list \n");
			nodeInsert(newNodeList, tempNodeList[i].firstArray, tempNodeList[i].firstCol, tempNodeList[i].secondArray, tempNodeList[i].secondCol, tempNodeList[i].commandType);
			offsetNodeCount +=1;


			/* ############# NULL COST -> RETURN 0 ################ */
			if(tempNodeList[i].fA == 0)
				return 0;


		}else{
			
			continue; //filters ended
		}


	}

//	printf("offset is %d\n", offsetNodeCount);

	bool *arrayCheckList = malloc(sizeof(bool)*commandListHead->arrayCount);

	memset(arrayCheckList, 0, sizeof(bool)*commandListHead->arrayCount);

	uint64_t a1,c1,a2,c2;

	bubbleNodeSort(tempNodeList, commandListHead->nodeCounter, offsetNodeCount);
	//adding the first - least cost node to our list
	nodeInsert(newNodeList, tempNodeList[offsetNodeCount].firstArray, tempNodeList[offsetNodeCount].firstCol, tempNodeList[offsetNodeCount].secondArray, tempNodeList[offsetNodeCount].secondCol, tempNodeList[offsetNodeCount].commandType);
	updateArrayStats(&tempNodeList[offsetNodeCount], tempArray, commandListHead->arraylist);
	
//####################################
	if(tempNodeList[offsetNodeCount].fA == 0)
		return 0;


	//saving prev array info , so we can add next connecting one later
	
	arrayCheckList[tempNodeList[offsetNodeCount].firstArray] = true;
	arrayCheckList[tempNodeList[offsetNodeCount].secondArray] = true;

	a1 = tempNodeList[offsetNodeCount].firstArray;
	a2 = tempNodeList[offsetNodeCount].secondArray;
	c1 = tempNodeList[offsetNodeCount].firstCol;
	c2 = tempNodeList[offsetNodeCount].secondCol;

	offsetNodeCount += 1;
	/**********prepei na to ulopoihsw updateListCost; ******************************/
	/*********************** shmantiko ***********************************************/

	//printf("arraycount is %d \n", commandListHead->arrayCount);
	while(offsetNodeCount<commandListHead->nodeCounter){//while we havent added all the nodes to our new list


		for(int j=offsetNodeCount;j<commandListHead->nodeCounter;j++){
			updateNodeCost(&tempNodeList[i], tempArray, commandListHead->arraylist);
		}

		bubbleNodeSort(tempNodeList, commandListHead->nodeCounter, offsetNodeCount);


		for(int i=offsetNodeCount;i<commandListHead->nodeCounter;i++){



			if(arrayCheckList[tempNodeList[i].firstArray] == true || arrayCheckList[tempNodeList[i].secondArray] == true){

				nodeInsert(newNodeList, tempNodeList[i].firstArray, tempNodeList[i].firstCol, tempNodeList[i].secondArray, tempNodeList[i].secondCol, tempNodeList[i].commandType);

				if(tempNodeList[i].fA == 0)
					return 0;


				arrayCheckList[tempNodeList[i].firstArray] = true;
				arrayCheckList[tempNodeList[i].secondArray] = true;

				a1 = tempNodeList[i].firstArray;
				a2 = tempNodeList[i].secondArray;
				c1 = tempNodeList[i].firstCol;
				c2 = tempNodeList[i].secondCol;
				//update stats for tempNodeList[i] here 
				updateArrayStats(&tempNodeList[i], tempArray, commandListHead->arraylist);
				
				nodeListSwap(&tempNodeList[offsetNodeCount], &tempNodeList[i]);

				offsetNodeCount += 1;
				break;
			}



		}

	}


	free(commandListHead->nodes);
	commandListHead->nodes = newNodeList;

	for(int i=0;i<tempArray->arrayCount;i++){

		free(tempArray->array[i].colStats); 		
		
	}

	free(tempArray->array);
	free(tempArray);
	free(arrayCheckList);


	return 1; 

}

void updateNodeCost(NODE *command, ARRAYSTATISTICS *stats, 	Arrays* arraylist){


	int firstArray = command->firstArray;
	int firstCol = command->firstCol;
	int secondCol = command->secondCol;
	int range = 0;
	int k = 0;
	int k2 = 0;
	int i = 0;

	Arrays* tempList = arraylist;


	while(i<firstArray){
		tempList = tempList->next;
		i++;
	}

	firstArray = tempList->arrayname;

	i = 0;
	tempList = arraylist;
	int secondArray = command->secondArray;
	while(i<secondArray){
		tempList = tempList->next;
		i++;
	}

	secondArray = tempList->arrayname;



	if(command->secondArray == -1){

		k = command->secondCol;

		uint64_t new_dA, old_fA;

		command->lA = stats->array[firstArray].colStats[firstCol].lA;
		command->uA = stats->array[firstArray].colStats[firstCol].uA;
		command->dA = stats->array[firstArray].colStats[firstCol].dA;
		command->fA = stats->array[firstArray].colStats[firstCol].fA;


		if(command->commandType == 0){//=

			range = command->uA - command->lA + 1;
			
			if(k > command->uA){
			
				old_fA = command->fA;
				new_dA = command->dA;
				command->dA = 0;
				command->fA = 0;

			}else{
			
				old_fA = command->fA;
				new_dA = command->dA;
				if((stats->array[firstArray].colStats[firstCol].arrayCheck[command->uA-k]) == true){
					command->dA = 1;
					command->fA = command->fA/command->dA;
				}else{
					command->dA = 0;
					command->fA = 0;
				}
			}

			//printf("cost is %ld \n", command->fA);

			//na kanw gia >49999999 


		}else if(command->commandType == 2){//<

			old_fA = command->fA;
			new_dA = command->dA;
			k2 = command->secondCol;
			k = command->lA;
			if(k2 < command->uA)
				k2 = command->uA;

			command->dA = ((k2-k)/(command->uA-command->lA))*command->dA;	

			command->fA = ((k2-k)/(command->uA-command->lA))*command->fA;	

			command->lA = k;
			command->uA = k2;


			//printf("cost is %ld \n", command->fA);

		}else if(command->commandType == 1){//>
			old_fA = command->fA;
			new_dA = command->dA;
			k2 = command->uA;
			if(k > command->lA)
				k = command->lA;

			command->dA = ((k2-k)/(command->uA-command->lA))*command->dA;	

			command->fA = ((k2-k)/(command->uA-command->lA))*command->fA;	

			command->lA = k;
			command->uA = k2;

			//printf("cost is %ld \n", command->fA);


		}else{
			printf("error in command type of filter (cost func) \n");
		}

		

		stats->array[firstArray].colStats[firstCol].lA = command->lA;
		stats->array[firstArray].colStats[firstCol].uA = command->uA;
		stats->array[firstArray].colStats[firstCol].dA = command->dA;
		stats->array[firstArray].colStats[firstCol].fA = command->fA;


		/* update the rest of arrays columns after applying filter */
		
		for(int i=0;i<stats->array[firstArray].columns;i++){
			if(i != command->firstCol){

				if (command->fA != 0)
					new_dA = new_dA*(1-(pow((1-(command->fA/old_fA)),(stats->array[firstArray].colStats[i].fA/stats->array[firstArray].colStats[i].dA))));
				else{
					new_dA = 0;
				}

				stats->array[firstArray].colStats[i].fA = command->fA;

			}
		}



	}else if(command->firstArray == command->secondArray){

		printf("\n\n!!!!!!!!!!!!!!!!!!Mpenei!!!!!!!!!!!!!!!!!!!!\n\n");


	}else{



		//printf("[%ld][%ld] %c [%ld][%ld] \n", command->firstArray, command->firstCol, com, command->secondArray, command->secondCol);
		//printf("%d %d \n", firstArray, secondArray);

		command->lA = stats->array[firstArray].colStats[firstCol].lA;
		command->uA = stats->array[firstArray].colStats[firstCol].uA;
		command->dA = stats->array[firstArray].colStats[firstCol].dA;
		command->fA = stats->array[firstArray].colStats[firstCol].fA;

		uint64_t lA2;
    	uint64_t uA2;
   		uint64_t fA2;
    	uint64_t dA2;
    	int n = 0;
    	int ab_flag = 0; //if 0, then attribute belongs to a, if 1 to b

		lA2 = stats->array[secondArray].colStats[secondCol].lA;
		uA2 = stats->array[secondArray].colStats[secondCol].uA;
		dA2 = stats->array[secondArray].colStats[secondCol].dA;
		fA2 = stats->array[secondArray].colStats[secondCol].fA;


		if(command->lA < lA2){
			command->lA = lA2;
		}else{
			lA2 = command->lA;
			ab_flag = 1;
		}


		if(command->uA > uA2){
			command->uA = uA2;
			ab_flag = 1;
		}else{
			uA2 = command->uA;
		}

		n = (uA2-lA2)+1;
		command->fA = (command->fA*fA2)/n;
		command->dA = (command->dA*dA2)/n;


		//printf("cost is %ld \n", command->fA);

	}




}


void updateArrayStats(NODE *command, ARRAYSTATISTICS *stats, 	Arrays* arraylist){

	
	//printf("updating stats for [%ld] [%ld] [%ld] [%ld]\n", command->firstArray, command->firstCol, command->secondArray, command->secondCol);


	uint64_t firstArray, secondArray, firstCol, secondCol;
	int i=0;

	Arrays* tempList = arraylist;

	firstArray = command->firstArray;

	while(i<firstArray){
		tempList = tempList->next;
		i++;
	}

	firstArray = tempList->arrayname;

	i = 0;
	tempList = arraylist;
	
	secondArray = command->secondArray;
	

	while(i<secondArray){
		tempList = tempList->next;
		i++;
	}

	secondArray = tempList->arrayname;

	firstCol = command->firstCol;
	secondCol = command->secondCol;

	uint64_t old_fA, old_dA;


	if(command->firstArray == command->secondArray){
		printf("\n!!!self join!!!\n");
	}else{


		//command->lA = stats->array[firstArray].colStats[firstCol].lA;

		stats->array[firstArray].colStats[firstCol].fA = command->fA;
		stats->array[firstArray].colStats[firstCol].dA = command->dA;
		stats->array[firstArray].colStats[firstCol].uA = command->uA;		
		stats->array[firstArray].colStats[firstCol].lA = command->lA;

		for(int i=0;i<stats->array[firstArray].columns;i++){

			if(i != firstCol){

				old_fA = stats->array[firstArray].colStats[i].fA;
				stats->array[firstArray].colStats[i].fA = command->fA;
				old_dA = stats->array[firstArray].colStats[i].dA;
				//stats->array[firstArray].colStats[i].dA = stats->array[firstArray].colStats[i].dA*(1-pow((1-(command->dA/old_dA)),(old_fA/old_dA)));

			}

		}

		stats->array[secondArray].colStats[secondCol].fA = command->fA;
		stats->array[secondArray].colStats[secondCol].dA = command->dA;
		stats->array[secondArray].colStats[secondCol].uA = command->uA;		
		stats->array[secondArray].colStats[secondCol].lA = command->lA;

		for(int i=0;i<stats->array[secondArray].columns;i++){

			if(i != firstCol){

				old_fA = stats->array[secondArray].colStats[i].fA;
				stats->array[secondArray].colStats[i].fA = command->fA;
				old_dA = stats->array[secondArray].colStats[i].dA;
				//stats->array[firstArray].colStats[i].dA = stats->array[firstArray].colStats[i].dA*(1-pow((1-(command->dA/old_dA)),(old_fA/old_dA)));

			}

		}





	}









}


void nodeListSwap(NODE* node1, NODE* node2){

	NODE *temp = malloc(sizeof(NODE));
	memcpy(temp, node1, sizeof(NODE));
	memcpy(node1, node2, sizeof(NODE));
	memcpy(node2, temp, sizeof(NODE));

	free(temp);
}

void bubbleNodeSort(NODE *tempNodeList, int count, int offset){

	count = count+offset;

	for(int i=offset;i<count-1;i++){
		for(int j=offset;j<count-i-1;j++){

			if(tempNodeList[j].fA > tempNodeList[j+1].fA){
				nodeListSwap(&tempNodeList[j], &tempNodeList[j+1]);
			}

		}
	}

}

