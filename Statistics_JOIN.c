#include "header.h"

float power(float base, int n){
	int i;
	float number;
	
	number=1;
	for(i=0 ; i< n ; i++)
		number =number*base;
	
	return number;
}

void CopyStats(Statistics** initStats,Statistics** stats,int* array,Initial_Table *Table,int num_of_files){
	// Create array with the initial Statistics
	int i,j;

	for(i=0 ; i<num_of_files ; i++){
		for(j=0 ; j< Table[array[i]].columns ; j++){
			stats[i][j].Ia = initStats[array[i]][j].Ia;
			stats[i][j].Ua = initStats[array[i]][j].Ua;
			stats[i][j].Fa = initStats[array[i]][j].Fa;
			stats[i][j].Da = initStats[array[i]][j].Da;
			stats[i][j].check = initStats[array[i]][j].check;
		}
	}
	
}

void DeleteStats(Statistics** stats,int num_of_files){
	int i;
	for(i=0 ; i<num_of_files ; i++)
		free(stats[i]);
		
}

void DeleteinitStats(Statistics** stats,int num_of_files,Initial_Table* Table){
	int i,j;
	for(i=0 ; i<num_of_files ; i++){
		
		for(j=0 ; j<Table[i].columns ; j++)
			free(stats[i][j].check);
			
		free(stats[i]);
	}
}


void InformStatistics(Statistics** stats,int first_array, int first_col,uint64_t first_num_cols, int sec_array, int sec_col,uint64_t sec_num_cols, int type){

	int i;
	float Pow;
	float temp;
	int64_t k1,k2;
	int64_t n,Ia,Ua,Fa,Da,Ic,Uc,Fc,Dc;
	int64_t Ib,Ub,Fb,Db;
	
	if(sec_array==-1){				// Filter
		
		if(type==0){				// equal
			
			if(stats[first_array][first_col].check[sec_col-stats[first_array][first_col].Ia]==true){
				stats[first_array][first_col].Fa= stats[first_array][first_col].Fa/ stats[first_array][first_col].Da;
				stats[first_array][first_col].Da=1;
			}
			else{
				stats[first_array][first_col].Fa=0;
				stats[first_array][first_col].Da=0;
			}

			stats[first_array][first_col].Ia= sec_col;
			stats[first_array][first_col].Ua= sec_col;
		}
		else{
		
			if(type==2){						// smaller than
				
				k1=stats[first_array][first_col].Ia;
			
				if(sec_col>stats[first_array][first_col].Ua)
					k2 = stats[first_array][first_col].Ua;
				else
					k2 = sec_col;
			
			}
			else if(type==1){						//bigger than
		
				
				k2=stats[first_array][first_col].Ua;
				
				if(sec_col<stats[first_array][first_col].Ia)
					k1 = stats[first_array][first_col].Ia;
				else
					k1 = sec_col;
			
			}
		
			stats[first_array][first_col].Ia = k1;
			stats[first_array][first_col].Ua = k2;
		
			temp= ((k2-k1) /(float) (stats[first_array][first_col].Ua-stats[first_array][first_col].Ia));
			stats[first_array][first_col].Da = stats[first_array][first_col].Da * temp;
			stats[first_array][first_col].Fa = stats[first_array][first_col].Fa * temp;
			
		
		}
		
		
		for(i=0; i< first_num_cols ; i++)				// for the rest columns
			if(i!=first_col){
					
				temp= stats[first_array][first_col].Fa/ (float)stats[first_array][first_col].Fa;
				
			
				Pow = power(1-temp,stats[first_array][i].Fa / stats[first_array][i].Da);
			
				stats[first_array][i].Da=stats[first_array][i].Da * (1-Pow );
				if(stats[first_array][i].Da==0)
					stats[first_array][i].Da=1;						
				stats[first_array][i].Fa= stats[first_array][first_col].Fa; 
					
				stats[first_array][i].Ia= stats[first_array][i].Ia;
				stats[first_array][i].Ua= stats[first_array][i].Ua;
				
			
				
			}
			
	}
	else{
		if(first_array == sec_array){								// Self Join

			
			Ia=stats[first_array][first_col].Ia;
			Ua=stats[first_array][first_col].Ua;
			Fa=stats[first_array][first_col].Fa;
			Da=stats[first_array][first_col].Da;
		
			Ib=stats[sec_array][sec_col].Ia;
			Ub=stats[sec_array][sec_col].Ua;
			Fb=stats[sec_array][sec_col].Fa;
			Db=stats[sec_array][sec_col].Da;
			
			if(Ia < Ib){
				stats[first_array][first_col].Ia= Ib;
				stats[first_array][sec_col].Ia= Ib;
			}
			else{
				stats[first_array][first_col].Ia= Ia;
				stats[first_array][sec_col].Ia= Ia;
			}
					
			if( Ua > Ub){
				stats[first_array][first_col].Ua= Ub;
				stats[first_array][sec_col].Ua= Ub;
			}
			else{
				stats[first_array][first_col].Ua= Ua;
				stats[first_array][sec_col].Ua= Ua;
			}
					
		
			n= stats[first_array][first_col].Ua - stats[first_array][first_col].Ia +1;
				
			stats[first_array][first_col].Fa = stats[first_array][sec_col].Fa = Fa / n;
				
			temp= stats[first_array][first_col].Fa/ (float)Fa;
					
			Pow = power(1-temp, Fa / Da);
						
			stats[first_array][first_col].Da= stats[first_array][sec_col].Da = Da * (1-Pow );
						
			for(i=0; i< first_num_cols ; i++)				// for the rest columns
				if(i!=first_col && i!=sec_col){
					
					
					Ic=stats[first_array][i].Ia;
					Uc=stats[first_array][i].Ua;
					Fc=stats[first_array][i].Fa;
					Dc=stats[first_array][i].Da;
					
					
					temp= stats[first_array][first_col].Fa/ (float)Fa;
						
					Pow = power(1-temp, Fc / Dc);
						
					stats[first_array][i].Da= Dc * (1-Pow );
											
					stats[first_array][i].Fa= stats[first_array][first_col].Fa; 
						
					stats[first_array][i].Ia= Ic;
					stats[first_array][i].Ua= Uc;
										
				}
			
		}
		else{													// join
			
		
			Ia=stats[first_array][first_col].Ia;
			Ua=stats[first_array][first_col].Ua;
			Fa=stats[first_array][first_col].Fa;
			Da=stats[first_array][first_col].Da;
					
			
			Ib=stats[sec_array][sec_col].Ia;
			Ub=stats[sec_array][sec_col].Ua;
			Fb=stats[sec_array][sec_col].Fa;
			Db=stats[sec_array][sec_col].Da;
			
			if(Ia>Ib)
				k1=Ia;
			else
				k1=Ib;
			
			if(Ua>Ub)
				k2=Ub;
			else
				k2=Ua;
				
			temp= ((k2-k1) /(float) (Ua-Ia));
			Da = Da * temp;
			Fa = Fa * temp;

			temp= ((k2-k1) /(float) (Ub-Ib));
			Db = Db * temp;
			Fb = Fb * temp;
				
			Ia=Ib=k1;
			Ua=Ib=k2;
									
			n=Ua-Ia+1;
			
			stats[first_array][first_col].Ia=stats[sec_array][sec_col].Ia = Ia;
			stats[first_array][first_col].Ua=stats[sec_array][sec_col].Ua = Ua;
			stats[first_array][first_col].Fa=stats[sec_array][sec_col].Fa = (Fa*Fb)/n;
			stats[first_array][first_col].Da=stats[sec_array][sec_col].Da = (Da*Db)/n;
					
			
			for(i=0; i< first_num_cols ; i++)				// for the rest columns
				if(i!=first_col){
					
					Ic=stats[first_array][i].Ia;
					Uc=stats[first_array][i].Ua;
					Fc=stats[first_array][i].Fa;
					Dc=stats[first_array][i].Da;
					
					if(Fa==0)
						stats[first_array][i].Da=0;
					else{
					
						temp= stats[first_array][first_col].Fa/ (float)Fa;
						if(Dc==0)
							Pow=power(1-temp,Fc);
						else
							Pow = power(1-temp, Fc / Dc);
						
						stats[first_array][i].Da= Dc * (1-Pow );
					}
									
											
					stats[first_array][i].Fa=  Fa; 
						
					stats[first_array][i].Ia= Ic;
					stats[first_array][i].Ua= Uc;
										
				}

			for(i=0; i< sec_num_cols ; i++)				// for the rest columns
				if(i!=sec_col){
					
					Ic=stats[sec_array][i].Ia;
					Uc=stats[sec_array][i].Ua;
					Fc=stats[sec_array][i].Fa;
					Dc=stats[sec_array][i].Da;
					
					if(Fb==0)
						stats[sec_array][i].Da=0;
					else{				
						temp= stats[sec_array][sec_col].Fa/ (float)Fb;
						if(Dc==0)
							Pow=power(1-temp,Fc);
						else
							Pow = power(1-temp, Fc / Dc);
							
						stats[sec_array][i].Da= Dc * (1-Pow );
					}
					stats[sec_array][i].Fa=  Fb; 
						
					stats[sec_array][i].Ia= Ic;
					stats[sec_array][i].Ua= Uc;
					
				}	
		}
	}	

}

int64_t checkStats(Statistics** stats,int first_array, int first_col, int sec_array, int sec_col){
	
	float temp;
	int64_t k1,k2;
	int64_t n,Ia,Ua,Fa,Da;
	int64_t Ib,Ub,Fb,Db;
	int counter=0;
		
	Ia=stats[first_array][first_col].Ia;
	Ua=stats[first_array][first_col].Ua;
	Fa=stats[first_array][first_col].Fa;
	Da=stats[first_array][first_col].Da;
	
	Ib=stats[sec_array][sec_col].Ia;
	Ub=stats[sec_array][sec_col].Ua;
	Fb=stats[sec_array][sec_col].Fa;
	Db=stats[sec_array][sec_col].Da;
	
	if(Ia>Ib)
		k1=Ia;
	else
		k1=Ib;
		
	if(Ua>Ub)
		k2=Ub;
	else
		k2=Ua;		
				
	temp= ((k2-k1) /(float) (Ua-Ia));
	Fa = Fa * temp;
	
	temp= ((k2-k1) /(float) (Ub-Ib));
	int64_t newFb;
	newFb = Fb * temp;
				
	Ia=k1;
	Ua=k2;
	n=Ua-Ia+1;
	return (Fa*newFb)/n;

}

int Evaluate(COMMANDLIST *command ,Statistics** initStats, Statistics** stats,int num_joins,int num_filters,int* array,Initial_Table* Table){

	NODE *initA = command->nodes->next;
	int i,num_of_files=command->num_of_files;
	

	for(i=0; i<num_filters; i++)
		initA=initA->next;
	
	NODE *initB, *A,*B,*point,*nodeA,*nodeB;
	A=initA;

	B=A->next;
	initB=B;
	if(initB == NULL)			//only one join in the list
		return 0;
	


	int counter=0;
	int64_t score, min=9999999999,Min=9999999999;

	if(num_joins ==2 ){			// only two joins in the list
		
		min=checkStats(stats, A->firstArray,A->firstCol,A->secondArray, A->secondCol);
	
		score=checkStats(stats, B->firstArray,B->firstCol,B->secondArray, B->secondCol);
	
		if(score < min)
			swapWorkNodes(A, B);
	
		return;
		
	}
	
	while(A!=NULL){
		B=A->next;
		InformStatistics(stats, A->firstArray,A->firstCol,Table[array[A->firstArray]].columns, A->secondArray, A->secondCol,Table[array[A->secondArray]].columns,A->commandType);
		while(B!=NULL){
			if(A->firstArray==B->firstArray || A->firstArray==B->secondArray || A->secondArray==B->firstArray || A->secondArray==B->secondArray){
				score=checkStats(stats, B->firstArray,B->firstCol,B->secondArray, B->secondCol);
				if(score == 0)
					return -1;
				if(score<min){
					point=B;
					min=score;
				}
			}			
			B=B->next;
		}
		
		CopyStats(initStats,stats,array,Table,num_of_files);
	
		if(min<Min){
			nodeA=A;
			nodeB=point;
			Min=min;
		}
		min=9999999999;
		A=A->next;
	}
		
	if(nodeA != initA)
		swapWorkNodes(nodeA, initA);
	if(nodeB !=initB)
		swapWorkNodes(nodeB, initB);
	
	return 0;	
}

int newSort(COMMANDLIST *commandListHead,int* array, Initial_Table* Table,Statistics** initStats, Statistics** stats){

	NODE *cur = commandListHead->nodes->next;
	
	int num_of_files=commandListHead->num_of_files;
	int num_filters=0;

	if(cur==NULL)
		return 0;

			
	// Inform statistics for all filters
	while( cur!=NULL && cur->secondArray == -1 ){
		InformStatistics(stats, cur->firstArray,cur->firstCol,Table[array[cur->firstArray]].columns, -1, cur->secondCol,0,cur->commandType);
		num_filters++;
		cur = cur->next;
	}

	// Inform statistics for all self joins
	while(cur != NULL && cur->firstArray == cur->secondArray){	
		InformStatistics(stats, cur->firstArray,cur->firstCol,Table[array[cur->firstArray]].columns, cur->secondArray, cur->secondCol,Table[array[cur->secondArray]].columns,cur->commandType);
		cur = cur->next;
	}


	int num_joins=0;
	while(cur!=NULL){
		num_joins++;
		cur=cur->next;
	}
	
	// Sort the joins
	if (Evaluate(commandListHead, initStats, stats, num_joins,num_filters, array, Table) == -1)
		return -1;
		
}
