#include "header.h"

void ReadFilesRecords(char* filename,uint64_t*** arrayname,Initial_Table* Table,int num_of_files){
	
	int i,j;
	FILE* fp = fopen(filename, "rb");// These files are binary
	if(fp == NULL)
		printf("ERROR: Could not open the file %s (ReadFilesRecords).\n", filename);

	fread(&(Table[num_of_files].rows), sizeof(uint64_t), 1, fp);// First we read the number of rows
	fread(&(Table[num_of_files].columns), sizeof(uint64_t), 1, fp);// After that we read the number of columns

	arrayname[num_of_files]= malloc(sizeof(uint64_t)*Table[num_of_files].columns);// We allocate memory for this array of records	
	Table[num_of_files].pointers=arrayname[num_of_files]; 

	for(i=0;i<Table[num_of_files].columns; i++){
		arrayname[num_of_files][i]=malloc(sizeof(uint64_t)* Table[num_of_files].rows);
		Table[num_of_files].pointers[i]=arrayname[num_of_files][i];// Each pointer points to the first element of a column
	}
				
	for(i=0;i<Table[num_of_files].columns; i++)
		for(j=0;j<Table[num_of_files].rows; j++)
			fread(&arrayname[num_of_files][i][j], sizeof(uint64_t), 1, fp);// Read and save all the records

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

void ReadInputFiles(char* File,uint64_t*** arrayname,Initial_Table* Table,int Flag){

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
		ReadFilesRecords(path,arrayname,Table,num_of_files);		
					
		num_of_files++;
	}
	free(line);
	fclose(Fp);
}