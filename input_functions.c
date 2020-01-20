#include "header.h"

void ReadFilesRecords(char* filename,uint64_t*** arrayname,Initial_Table* Table,int num_of_files,Statistics** initStats){
	
	int i,j;
	uint64_t N=6700417;
	uint64_t min,max;
	
	FILE* fp = fopen(filename, "rb");// These files are binary
	if(fp == NULL)
		printf("ERROR: Could not open the file %s (ReadFilesRecords).\n", filename);

	fread(&(Table[num_of_files].rows), sizeof(uint64_t), 1, fp);// First we read the number of rows
	fread(&(Table[num_of_files].columns), sizeof(uint64_t), 1, fp);// After that we read the number of columns

	arrayname[num_of_files]= malloc(sizeof(uint64_t)*Table[num_of_files].columns);// We allocate memory for this array of records	
	Table[num_of_files].pointers=arrayname[num_of_files]; 

	initStats[num_of_files]=malloc(sizeof(Statistics)*Table[num_of_files].columns);	// We need for each column of file to find statistics
	

	for(i=0;i<Table[num_of_files].columns; i++){
		arrayname[num_of_files][i]=malloc(sizeof(uint64_t)* Table[num_of_files].rows);
		Table[num_of_files].pointers[i]=arrayname[num_of_files][i];// Each pointer points to the first element of a column
	}
				
	uint64_t number;		
	for(i=0;i<Table[num_of_files].columns; i++){
		min=9446744073709551614;
		max=1;
		for(j=0;j<Table[num_of_files].rows; j++){
		
			fread(&arrayname[num_of_files][i][j], sizeof(uint64_t), 1, fp);// Read and save all the records
			if(arrayname[num_of_files][i][j]<min)
				min=arrayname[num_of_files][i][j];
			if(arrayname[num_of_files][i][j]>max)
				max=arrayname[num_of_files][i][j];
		}
		
		
		initStats[num_of_files][i].Ia=min;
		initStats[num_of_files][i].Ua=max;
		
		initStats[num_of_files][i].Fa=Table[num_of_files].rows;
		
		// Work to find Da
		number=max - min + 1;
		uint64_t rows;
		if(number<=N)
			rows=number;
		else
			rows=N;
		
		initStats[num_of_files][i].check=malloc(sizeof(bool)*rows);
		uint64_t counter=0;	
		
		for(j = 0; j < rows ; j++)
			initStats[num_of_files][i].check[j]=false;
			
		
		for(j=0;j<Table[num_of_files].rows; j++)
			if(number<=N && initStats[num_of_files][i].check[arrayname[num_of_files][i][j]-min]== false){
				counter++;
				initStats[num_of_files][i].check[arrayname[num_of_files][i][j]-min]= true;
			}
			else if(number>N && initStats[num_of_files][i].check[(arrayname[num_of_files][i][j]-min)%N]== false){
				counter++;
				initStats[num_of_files][i].check[(arrayname[num_of_files][i][j]-min)%N]= true;
			}
			
		
		/*for(j=0; j<rows ; j++)
			if(initStats[num_of_files][i].check[j]==true)
				counter++;*/
		
		initStats[num_of_files][i].Da=counter;
	}
			
	fclose(fp);
}

int Countrows(char* filename){
	
	// Count rows of a file
	FILE* fp1;
	fp1 = fopen(filename, "r");
	if(fp1 == NULL)
		printf("ERROR: Could not open the file %s (CountRows).\n", filename);
	
	char* line=NULL, *token=NULL;
	size_t len = 0;
	int num_of_files=0;
	
	// Count the number of files
	while(getline(&line, &len, fp1) != -1)
		num_of_files++;

	free(line);
	fclose(fp1);
	return num_of_files;
}

void ReadInputFiles(char* File,uint64_t*** arrayname,Initial_Table* Table,int Flag,Statistics** stats){

	FILE* Fp;
	Fp = fopen(File, "r");
	if(Fp == NULL)
		printf("ERROR: Could not open the file %s (ReadInputFiles).\n", File);

	char* line=NULL;
	size_t len = 0;

	char filename[100];
	int i,j;
	int num_of_files=0;
	char path[100];
	
	// Read again the initial file and open one by one the files with records
	while(getline(&line, &len, Fp) != -1){
				
		sscanf(line,"%s", filename);
		if(Flag==1)
			strcpy(path,"/tmp/workloads/small/");
		else if(Flag==2)
			strcpy(path,"/tmp/workloads/medium/");
		else if(Flag == 3)
			strcpy(path,"");

		strcat(path,filename);
		ReadFilesRecords(path,arrayname,Table,num_of_files,stats);		
					
		num_of_files++;
	}
	free(line);
	fclose(Fp);
}
