/************************************************************
Project#1:	CLP & DDL
************************************************************/

#include "db.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <math.h>
#include <time.h>

table_file_header *table_file; // for inserting values into table file
//token_list *for_log;
char *query=NULL;

int main(int argc, char** argv)
{
	int rc = 0;
	token_list *tok_list=NULL, *tok_ptr=NULL, *tmp_tok_ptr=NULL;
	//argv[1]="db";
	query=argv[1];

	if ((argc != 2) || (strlen(argv[1]) == 0))
	{
		printf("Usage: db \"command statement\"");
		return 1;
	}

	rc = initialize_tpd_list();

	if (rc)
	{
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
	}
	else
	{
		rc = get_token(argv[1], &tok_list);

		/* Test code */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			printf("%16s \t%d \t %d\n",tok_ptr->tok_string, tok_ptr->tok_class,
				tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}

		if (!rc)
		{
			rc = do_semantic(tok_list);
		}

		if (rc)
		{
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				if ((tok_ptr->tok_class == error) ||
					(tok_ptr->tok_value == INVALID))
				{
					printf("\nError in the string: %s\n", tok_ptr->tok_string);
					printf("rc=%d\n", rc);
					break;
				}
				tok_ptr = tok_ptr->next;
			}
		}

		/* Whether the token list is valid or not, we need to free the memory */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			tmp_tok_ptr = tok_ptr->next;
			free(tok_ptr);
			tok_ptr=tmp_tok_ptr;
		}
	}

	return rc;
}

/************************************************************* 
This is a lexical analyzer for simple SQL statements
*************************************************************/
int get_token(char* command, token_list** tok_list)
{
	int rc=0,i,j;
	char *start, *cur, temp_string[MAX_TOK_LEN];
	bool done = false;

	start = cur = command;
	while (!done)
	{
		bool found_keyword = false;

		/* This is the TOP Level for each token */
		memset ((void*)temp_string, '\0', MAX_TOK_LEN);
		i = 0;

		/* Get rid of all the leading blanks */
		while (*cur == ' ')
			cur++;

		if (cur && isalpha(*cur))
		{
			// find valid identifier
			int t_class;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((isalnum(*cur)) || (*cur == '_'));

			if (!(strchr(STRING_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				is not a blank, (, ), or a comma, then append this
				character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{

				// We have an identifier with at least 1 character
				// Now check if this ident is a keyword
				for (j = 0, found_keyword = false; j < TOTAL_KEYWORDS_PLUS_TYPE_NAMES; j++)
				{
					if ((stricmp(keyword_table[j], temp_string) == 0))
					{
						found_keyword = true;
						break;
					}
				}

				if (found_keyword)
				{
					if (KEYWORD_OFFSET+j < K_CREATE)
						t_class = type_name;
					else if (KEYWORD_OFFSET+j >= F_SUM)
						t_class = function_name;
					else
						t_class = keyword;

					add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET+j);
				}
				else
				{
					if (strlen(temp_string) <= MAX_IDENT_LEN)
						add_to_list(tok_list, temp_string, identifier, IDENT);
					else
					{
						add_to_list(tok_list, temp_string, error, INVALID);
						rc = INVALID;
						done = true;
					}
				}

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if (isdigit(*cur))
		{
			// find valid number
			do 
			{
				temp_string[i++] = *cur++;
			}
			while (isdigit(*cur));

			if (!(strchr(NUMBER_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				is not a blank or a ), then append this
				character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{
				add_to_list(tok_list, temp_string, constant, INT_LITERAL);

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if ((*cur == '(') || (*cur == ')') || (*cur == ',') || (*cur == '*')
			|| (*cur == '=') || (*cur == '<') || (*cur == '>'))
		{
			/* Catch all the symbols here. Note: no look ahead here. */
			int t_value;
			switch (*cur)
			{
			case '(' : t_value = S_LEFT_PAREN; break;
			case ')' : t_value = S_RIGHT_PAREN; break;
			case ',' : t_value = S_COMMA; break;
			case '*' : t_value = S_STAR; break;
			case '=' : t_value = S_EQUAL; break;
			case '<' : t_value = S_LESS; break;
			case '>' : t_value = S_GREATER; break;
			}

			temp_string[i++] = *cur++;

			add_to_list(tok_list, temp_string, symbol, t_value);

			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
		}
		else if (*cur == '\'')
		{
			/* Find STRING_LITERRAL */
			int t_class;
			cur++;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((*cur) && (*cur != '\''));

			temp_string[i] = '\0';

			if (!*cur)
			{
				/* If we reach the end of line */
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else /* must be a ' */
			{
				add_to_list(tok_list, temp_string, constant, STRING_LITERAL);
				cur++;
				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else
		{
			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
			else
			{
				/* not a ident, number, or valid symbol */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
		}
	}

	return rc;
}

void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value)
{
	token_list *cur = *tok_list;
	token_list *ptr = NULL;

	// printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

	ptr = (token_list*)calloc(1, sizeof(token_list));
	strcpy(ptr->tok_string, tmp);
	ptr->tok_class = t_class;
	ptr->tok_value = t_value;
	ptr->next = NULL;

	if (cur == NULL)
		*tok_list = ptr;
	else
	{
		while (cur->next != NULL)
			cur = cur->next;

		cur->next = ptr;
	}
	return;
}

int do_semantic(token_list *tok_list)
{
	int rc = 0, cur_cmd = INVALID_STATEMENT;
	bool unique = false;
	token_list *cur = tok_list;
	//for_log = tok_list;
	
	if ((cur->tok_value == K_CREATE) &&
		((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("CREATE TABLE statement\n");
		cur_cmd = CREATE_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_INSERT) &&
		((cur->next != NULL) && (cur->next->tok_value == K_INTO)))
	{
		printf("INSERT TABLE statement\n");
		cur_cmd = INSERT;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_SELECT) && (cur->next != NULL)) //&& (cur->next->tok_value == '*')))
	{

		printf("SELECT TABLE statement\n");
		cur_cmd = SELECT;
		cur = cur->next; //->next->next;

	}
	else if ((cur->tok_value == K_UPDATE) && (cur->next != NULL)) //&& (cur->next->tok_value == K_TABLE)))
	{
		printf("UPDATE TABLE statement\n");
		cur_cmd = UPDATE;
		cur = cur->next;
	}
	else if ((cur->tok_value == K_DELETE) &&
		((cur->next != NULL) && (cur->next->tok_value == K_FROM)))
	{
		printf("DELETE TABLE statement\n");
		cur_cmd = DELETE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_BACKUP) &&
		((cur->next != NULL) && (cur->next->tok_value == K_TO)))
	{
		printf("BACKUP DATABASE statement\n");
		cur_cmd = BACKUP;
		cur = cur->next->next;
	}

	else if ((cur->tok_value == K_RESTORE) &&
		((cur->next != NULL) && (cur->next->tok_value == K_FROM)))
	{
		printf("RESTORE DATABASE statement\n");
		cur_cmd = RESTORE;
		cur = cur->next->next;
	}

	else if ((cur->tok_value == K_DROP) &&
		((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("DROP TABLE statement\n");
		cur_cmd = DROP_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
		((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("LIST TABLE statement\n");
		cur_cmd = LIST_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
		((cur->next != NULL) && (cur->next->tok_value == K_SCHEMA)))
	{
		printf("LIST SCHEMA statement\n");
		cur_cmd = LIST_SCHEMA;
		cur = cur->next->next;
	}
	else
	{
		printf("Invalid statement\n");
		rc = cur_cmd;
	}

	if (cur_cmd != INVALID_STATEMENT)
	{
		switch(cur_cmd)
		{
		case CREATE_TABLE:
			rc = sem_create_table(cur);
			break;
		case DROP_TABLE:
			rc = sem_drop_table(cur);
			break;
		case LIST_TABLE:
			rc = sem_list_tables();
			break;
		case LIST_SCHEMA:
			rc = sem_list_schema(cur);
			break;
		case INSERT:
			rc = sem_insert(cur);
			break;
		case SELECT:
			rc = sem_select(cur);
			break;
		case UPDATE:
			rc = sem_update(cur);
			break;
		case DELETE:
			rc = sem_delete(cur);
			break;
		case BACKUP:
			rc = sem_backup(cur);
			break;
		case RESTORE:
			rc = sem_restore(cur);
		default:
			; /* no action */
		}
	}

	return rc;
}

int sem_create_table(token_list *t_list)
{
	int rc = 0;
	int tabfile_record_size=0;
	token_list *cur;
	//token_list *log_cur;
	tpd_entry tab_entry;
	tabfile_row *filerow;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry col_entry[MAX_NUM_COL];


	memset(&tab_entry, '\0', sizeof(tpd_entry));
	cur = t_list;

	if ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL)
		{
			rc = DUPLICATE_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			strcpy(tab_entry.table_name, cur->tok_string);    
			cur = cur->next;
			if (cur->tok_value != S_LEFT_PAREN)
			{
				//Error
				rc = INVALID_TABLE_DEFINITION;
				cur->tok_value = INVALID;
			}
			else
			{
				memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

				/* Now build a set of column entries */
				cur = cur->next;
				do
				{
					if ((cur->tok_class != keyword) &&
						(cur->tok_class != identifier) &&
						(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						int i;
						for(i = 0; i < cur_id; i++)
						{
							/* make column name case sensitive */
							if (strcmp(col_entry[i].col_name, cur->tok_string)==0)
							{
								rc = DUPLICATE_COLUMN_NAME;
								cur->tok_value = INVALID;
								break;
							}
						}

						if (!rc)
						{
							strcpy(col_entry[cur_id].col_name, cur->tok_string);
							col_entry[cur_id].col_id = cur_id;
							col_entry[cur_id].not_null = false;    /* set default */

							cur = cur->next;
							if (cur->tok_class != type_name)
							{
								// Error
								rc = INVALID_TYPE_NAME;
								cur->tok_value = INVALID;
							}
							else
							{
								/* Set the column type here, int or char */
								col_entry[cur_id].col_type = cur->tok_value;
								cur = cur->next;

								if (col_entry[cur_id].col_type == T_INT)
								{
									if ((cur->tok_value != S_COMMA) &&
										(cur->tok_value != K_NOT) &&
										(cur->tok_value != S_RIGHT_PAREN))
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										col_entry[cur_id].col_len = sizeof(int);
										tabfile_record_size = tabfile_record_size+1+4;
										if ((cur->tok_value == K_NOT) &&
											(cur->next->tok_value != K_NULL))
										{
											rc = INVALID_COLUMN_DEFINITION;
											cur->tok_value = INVALID;
										}	
										else if ((cur->tok_value == K_NOT) &&
											(cur->next->tok_value == K_NULL))
										{					
											col_entry[cur_id].not_null = true;
											cur = cur->next->next;
										}

										if (!rc)
										{
											/* I must have either a comma or right paren */
											if ((cur->tok_value != S_RIGHT_PAREN) &&
												(cur->tok_value != S_COMMA))
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												if (cur->tok_value == S_RIGHT_PAREN)
												{
													column_done = true;
												}
												cur = cur->next;
											}
										}
									}
								}   // end of S_INT processing
								else
								{
									// It must be char()
									if (cur->tok_value != S_LEFT_PAREN)
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										/* Enter char(n) processing */
										cur = cur->next;

										if (cur->tok_value != INT_LITERAL)
										{
											rc = INVALID_COLUMN_LENGTH;
											cur->tok_value = INVALID;
										}
										else
										{
											/* Got a valid integer - convert */
											col_entry[cur_id].col_len = atoi(cur->tok_string);
											tabfile_record_size = tabfile_record_size+1+atoi(cur->tok_string);
											cur = cur->next;

											if (cur->tok_value != S_RIGHT_PAREN)
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												cur = cur->next;

												if ((cur->tok_value != S_COMMA) &&
													(cur->tok_value != K_NOT) &&
													(cur->tok_value != S_RIGHT_PAREN))
												{
													rc = INVALID_COLUMN_DEFINITION;
													cur->tok_value = INVALID;
												}
												else
												{
													if ((cur->tok_value == K_NOT) &&
														(cur->next->tok_value != K_NULL))
													{
														rc = INVALID_COLUMN_DEFINITION;
														cur->tok_value = INVALID;
													}
													else if ((cur->tok_value == K_NOT) &&
														(cur->next->tok_value == K_NULL))
													{					
														col_entry[cur_id].not_null = true;
														cur = cur->next->next;
													}

													if (!rc)
													{
														/* I must have either a comma or right paren */
														if ((cur->tok_value != S_RIGHT_PAREN) &&															  (cur->tok_value != S_COMMA))
														{
															rc = INVALID_COLUMN_DEFINITION;
															cur->tok_value = INVALID;
														}
														else
														{
															if (cur->tok_value == S_RIGHT_PAREN)
															{
																column_done = true;
															}
															cur = cur->next;
														}
													}
												}
											}
										}	/* end char(n) processing */
									}
								} /* end char processing */
							}
						}  // duplicate column name
					} // invalid column name

					/* If rc=0, then get ready for the next column */
					if (!rc)
					{
						cur_id++;
					}

				} while ((rc == 0) && (!column_done));

				if ((column_done) && (cur->tok_value != EOC))
				{
					rc = INVALID_TABLE_DEFINITION;
					cur->tok_value = INVALID;
				}

				if (!rc)
				{
					/* Now finished building tpd and add it to the tpd list */
					tab_entry.num_columns = cur_id;
					tab_entry.tpd_size = sizeof(tpd_entry) + 
						sizeof(cd_entry) *	tab_entry.num_columns;
					tab_entry.cd_offset = sizeof(tpd_entry);
					new_entry = (tpd_entry*)calloc(1, tab_entry.tpd_size);

					if (new_entry == NULL)
					{
						rc = MEMORY_ERROR;
					}
					else
					{
						memcpy((void*)new_entry,
							(void*)&tab_entry,
							sizeof(tpd_entry));

						memcpy((void*)((char*)new_entry + sizeof(tpd_entry)),
							(void*)col_entry,
							sizeof(cd_entry) * tab_entry.num_columns);

						rc = add_tpd_to_list(new_entry);
						//double rounding;

						// rounding record size to 4 byte boundary

						//rounding = ceil(double(tabfile_record_size)/4);
						//tabfile_record_size = rounding*4;

						// writing values into the table file header
						table_file = NULL;
						table_file = (table_file_header*)calloc(1, sizeof(table_file_header));
						table_file->file_size = sizeof(table_file_header);
						table_file->record_size = tabfile_record_size;
						table_file->num_records=0;     // because we are just creating table
						table_file->record_offset = sizeof(table_file_header);
						rc = init_table_file(tab_entry.table_name);

						if (rc==0)
						{							
							rc = init_table_file(tab_entry.table_name);							
							if (rc==0)
								printf("Table file created is %s\n",tab_entry.table_name);
							else
								printf("Table file is not created\n");
						}
						free(new_entry);
					}
				}
			}
		}
	}
	if(rc==0)
	{
		int a=0;
		a=logging();

		if(a==0)
			return rc;
	}
	return rc;
}

int sem_insert(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	char filename[MAX_IDENT_LEN+4]; 
	tpd_entry *tab_entry;
	cd_entry *column;
	char *filerow;
	cur = t_list;
	FILE *fhandle = NULL;
	struct _stat file_stat;


	if ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}

	else
	{
		if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
		{
			rc = TABLE_NOT_EXIST;
			cur->tok_value = INVALID;
			return rc;
		}


		//opening the tablefile

		if(!rc)
		{
			strcpy(filename,cur->tok_string);
			strcat(filename,".tab");
			fhandle = fopen(filename, "rbc");
			_fstat(_fileno(fhandle), &file_stat);
			//printf("%s file size is %d\n",filename,file_stat.st_size);
			table_file = (table_file_header*)calloc(1, file_stat.st_size);

			if (!table_file)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				fread(table_file, file_stat.st_size, 1, fhandle);
				fclose(fhandle);
				if (table_file->file_size != file_stat.st_size)
				{
					rc = TABLEFILE_CORRUPTION;
				}



			}
		}

		//continue parsing

		cur = cur->next;
		if (cur->tok_value != K_VALUES)
		{
			//Error
			rc = INVALID_TABLE_DEFINITION;
			cur->tok_value = INVALID;
			return rc;
		}
		cur = cur->next;
		if (cur->tok_value != S_LEFT_PAREN)
		{
			//Error
			rc = INVALID_TABLE_DEFINITION;
			cur->tok_value = INVALID;
			return rc;
		}


		// handling row values

		cur = cur->next;
		int i=0 , tot_columns=tab_entry->num_columns;
		int offset=0, si=sizeof(int),char_size;
		int value;
		filerow = (char *)calloc(1, 1000); // 1000 because maximum allowed rows are 1000
		memset(filerow,'\0',1000);
		column = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
		char_size = column->col_len;
		while(i<tot_columns)
		{

			//handling NULL

			if (cur->tok_value == K_NULL)
			{
				if (column->not_null)
				{
					rc = NULL_IS_ASSIGNED;
					cur->tok_value = INVALID;
					return rc;
				}
				else
				{
					int nullSize=0;
					memcpy(filerow + offset, &nullSize, 1);
					offset = offset + 1+column->col_len;	
					cur = cur->next;
				}
			}

			// handling length 

			else if (strlen(cur->tok_string) > column->col_len)
			{
				rc = WRONG_VALUE_LENGTH;
				cur->tok_value = INVALID;
				return rc;
			}

			//handling integer values

			else if(column->col_type == T_INT && cur->tok_value != INT_LITERAL)
			{
				rc = INCORRECT_DATA_TYPE;
				cur->tok_value = INVALID;
				return rc;
			}

			else if(column->col_type == T_INT && cur->tok_value == INT_LITERAL)
			{
				memcpy(filerow+offset,&si,1); // 1 Byte is copied for storing the length
				offset = offset+1;
				value = atoi(cur->tok_string);
				memcpy(filerow+offset, &value,si);
				offset = offset+si;
				cur = cur->next;
			}

			// handling 'null' values


			//handling character values

			else if (cur->tok_value != STRING_LITERAL)
			{
				rc = INCORRECT_DATA_TYPE;
				cur->tok_value = INVALID;
				return rc;
			}

			else if (cur->tok_value == STRING_LITERAL)
			{

				char_size = column->col_len;
				memcpy(filerow+offset, &char_size, 1); // 1 Byte is copied for storing the length
				offset = offset+1;
				memcpy(filerow+offset,cur->tok_string,strlen(cur->tok_string));
				offset = offset+column->col_len;
				cur = cur->next;
			}

			// handling comma and right paranthesis

			if ((cur->tok_value != S_COMMA) && (cur->tok_value != S_RIGHT_PAREN))
			{
				rc = INVALID_COLUMN_DEFINITION;
				cur->tok_value = INVALID;
				return rc;
			}
			else
			{
				if((cur->tok_value == S_COMMA)) 
					cur=cur->next;
				
			}

			i = i+1;
			column = column+1;
		}

		if(cur->tok_value!=S_RIGHT_PAREN)
		{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
				return rc;
		}
		
		// writing the rows to the table file
		/*
		1) size of the table file should be increased
		2) Number of records should be incremented by 1 upon an addition of row 
		3) new row should be copied to file.
		*/
		table_file->num_records = table_file->num_records+1;
		int oldsize= table_file -> file_size;
		int recordsize = table_file -> record_size;
		table_file -> file_size += table_file->record_size;
		fhandle = NULL;
		fhandle = fopen(filename, "wbc");
		fwrite(table_file, oldsize, 1, fhandle);
		fwrite(filerow, recordsize, 1, fhandle);
		printf("\n The number of records are: %d",table_file->num_records);
		fflush(fhandle);
		fclose(fhandle);
		fhandle = fopen(filename, "rbc");
		_fstat(_fileno(fhandle), &file_stat);
		//printf("\n %s file size is %d\n",filename,file_stat.st_size);
		fclose(fhandle);

		if(rc==0)
		{
			int a=0;
			a=logging();

			if(a==0)
				return rc;
		}
		return rc;
	}
}	

int sem_select(token_list *t_list)
{
	int rc = 0,i=0,j=0;
	int col_ids[16];
	int cols_counter=0;
	bool star_bool=false;
	bool cols_bool=false;
	bool where_bool=false;
	bool count_bool=false;
	bool sum_bool=false;
	bool avg_bool=false;
	int count_counter=0;
	int null_counter=0;
	int sum=0;
	int equal_bool[2]={0,0};
	int great_bool[2]={0,0};
	int less_bool[2]={0,0};
	char *agg_value;
	char *cols[2];
	char *colv[2];
	char *oprtr[1];
	int reloprtr[2];
	int num_rows=0,num_columns=0;
	token_list *cur;
	token_list *temp_col;
	token_list *agg_where;
	char filename[MAX_IDENT_LEN+4]; 
	tpd_entry *tab_entry;
	tpd_entry *temp_tab_entry;
	tpd_entry *tab_ptr=NULL;
	cd_entry *column;
	cur = t_list;
	temp_col = t_list;
	agg_where = t_list;
	FILE *fhandle = NULL;
	struct _stat file_stat;
	int check=0;
	// handling select,count,*,where
	do
	{
		if((agg_where->tok_value==F_COUNT)||(agg_where->tok_value==S_STAR)||(agg_where->tok_value==K_WHERE))
		{
			agg_where=agg_where->next;
			check++;
		}
		agg_where=agg_where->next;
	}while(agg_where->tok_value!=EOC);

	if(check==3)
	{
		star_bool=true;
		count_bool=true;
		do
		{
			cur=cur->next;
		}while(cur->tok_value!=K_FROM);
		cur=cur->next;
	}
	// handling 'count'or'avg'or'sum','(','* or column_name',')', 'from'
	else if((cur->tok_value==F_COUNT)||(cur->tok_value==F_SUM)||(cur->tok_value==F_AVG))
	{
		if(cur->tok_value==F_COUNT)count_bool=true;
		if(cur->tok_value==F_SUM)sum_bool=true;
		if(cur->tok_value==F_AVG)avg_bool=true;
		cur = cur->next;
		
		// for two agg functions

		if(cur->tok_value!=S_LEFT_PAREN)
		{
			rc = INVALID_STATEMENT;
			return rc;
		}
		else
		{
			cur = cur->next;
			if((cur->tok_value==S_STAR)||(cur->tok_value==IDENT))
			{
				agg_value=cur->tok_string;
				if(cur->tok_value==S_STAR)star_bool=true;
				if(cur->tok_value==IDENT)
				{
					cols_bool=true;
					do
					{
						temp_col=temp_col->next;
					}while(temp_col->tok_value!=K_FROM);
					temp_col=temp_col->next;
					tab_entry = get_tpd_from_list(temp_col->tok_string);
					int n_columns=tab_entry->num_columns;
					//printf("%d",n_columns);
					int j=0;
					for(j=0;j<16;j++)
					{col_ids[j]=16;}

					//store that column id in an array
					int a=0,i=0;

					column = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);


					while(a<n_columns)
					{														
						if(!(stricmp(column->col_name,cur->tok_string)))
						{
							col_ids[a]=column->col_id;
							//printf("%d",col_ids[i]);
							break;
						}
						column++;		
						a++;
					}

				}
				cur=cur->next;
				if(cur->tok_value!=S_RIGHT_PAREN)
				{
					rc = INVALID_STATEMENT;
					return rc;
				}
				else
				{
					cur=cur->next;
					if(cur->tok_value!=K_FROM)
					{
						rc = INVALID_STATEMENT;
						return rc;
					}
					else
					{
						cur=cur->next;
					}
				}
			}
			else
			{
				rc = INVALID_STATEMENT;
				return rc;
			}
		}
	}

	// handling '*' and FROM

	else if (cur->tok_value == S_STAR)
	{
		cur = cur->next;
		star_bool = true;

		if(cur->tok_value == K_FROM)
		{
			cur = cur->next;
		}
		else
		{
			rc = INVALID_STATEMENT;
			return rc;
		}
	}

	// handling column_names,getting column id's and handling FROM 

	else if(cur->tok_value == IDENT)
	{
		cols_bool=true;
		do
		{
			if(temp_col->tok_value==S_COMMA)
			{
				temp_col=temp_col->next;
			}
			temp_col=temp_col->next;
		}while(temp_col->tok_value!=K_FROM);

		temp_col=temp_col->next;
		tab_entry = get_tpd_from_list(temp_col->tok_string);
		int n_columns=tab_entry->num_columns;
		//printf("%d",n_columns);
		for(j=0;j<16;j++)
		{col_ids[j]=16;}
		do
		{
			// skip comma
			if(cur->tok_value==S_COMMA)
			{
				cur=cur->next;
			}

			//store that column id in an array
			int a=0,i=0;

			column = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			//printf("%s",column->col_name);
			//printf("%d",column->col_id);


			while(a<n_columns)
			{														
				if(!(stricmp(column->col_name,cur->tok_string)))
				{
					col_ids[a]=column->col_id;
					//printf("%d",col_ids[i]);
					break;
				}
				column++;		
				a++;
			}

			cur=cur->next;
		}while(cur->tok_value!=K_FROM);
		cur=cur->next;
		//printf("%s",cur->tok_string);
	}

	else
	{
		rc = INVALID_STATEMENT;
		return rc;
	}


	// handling table name

	if ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
		return rc;
	}

	else
	{
		if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
		{
			rc = TABLE_NOT_EXIST;
			cur->tok_value = INVALID;
			return rc;
		}

		// finding the number of rows and number of columns in the file
		num_columns=tab_entry->num_columns;

		//opening the tablefile

		if(!rc)
		{
			strcpy(filename,cur->tok_string);
			strcat(filename,".tab");
			fhandle = fopen(filename, "rbc");
			_fstat(_fileno(fhandle), &file_stat);

			table_file = (table_file_header*)calloc(1, file_stat.st_size);

			if (!table_file)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				fread(table_file, file_stat.st_size, 1, fhandle);

				// finding the number of rows and number of columns in the file

				num_columns=tab_entry->num_columns;
				num_rows=table_file->num_records;

				//printf("\n columns:%d \n rows:%d",num_columns,num_rows);	


				fclose(fhandle);
				if (table_file->file_size != file_stat.st_size)
				{
					rc = TABLEFILE_CORRUPTION;
				}

			}
		}

		// handling 'where'
		cur = cur->next;
		if (cur->tok_value == K_WHERE)
		{
			where_bool = true;
			cur = cur->next;
			int p=0;
			do
			{
				if(cur->tok_value == IDENT)
				{
					cols[p] = cur->tok_string;
					cur = cur->next;
					// catch invalid relational operator
					/*if((cur->tok_value != S_EQUAL)&&(cur->tok_value != S_GREATER)&&(cur->tok_value != S_LESS)&&(cur->tok_value != K_IS))
					{
						rc = INVALID_RELATIONAL_OPERATOR;
						return rc;
					}*/
					if((cur->tok_value == S_EQUAL)||(cur->tok_value == S_GREATER)||(cur->tok_value == S_LESS)||(cur->tok_value == K_IS))
					{
						if((cur->tok_value==S_EQUAL)) 
						{	
							equal_bool[p]=1;
							//reloprtr[p]=1;
						}
						if((cur->tok_value==K_IS)) 
						{	
							if(cur->next->tok_value==K_NOT)
							{cur=cur->next;}
							equal_bool[p]=1;
							//reloprtr[p]=1;
						}
						if(cur->tok_value==S_GREATER)
						{	
							great_bool[p]=1;
							//reloprtr[p]=1;
						}
						if(cur->tok_value==S_LESS)
						{	
							less_bool[p]=1;
							//reloprtr[p]=1;
						}

						cur = cur->next;
						colv[p] = cur->tok_string;
						cols_counter++;
						cur = cur->next;


						if(cur->tok_value == EOC)
						{ 
							break;
						}
						if(cur->tok_value==K_AND || cur->tok_value==K_OR)
						{
							oprtr[p] = cur->tok_string;
							cur = cur->next;
							p++;

						}
						else
						{
							rc = INVALID_STATEMENT;
							return rc;
						}
					}
					// Invalid relational operator
					else
					{
						rc = INVALID_RELATIONAL_OPERATOR;
						cur->tok_value=INVALID;
						return rc;
					}
				}
			}while(cur->tok_value != EOC);

			/*else
			{
			rc = INVALID_STATEMENT;
			return rc;
			}*/

		}

		// handles select * from table

		if(star_bool && where_bool)
		{
			/*printf("where and star");
			int i=0;
			printf("\n");
			for(i=0;i<2;i++)
			{
			printf("%s\n",cols[i]);
			printf("%s\n",colv[i]);
			}
			printf("%s\n",oprtr[0]);
			for(i=0;i<2;i++)
			{
			printf("%d\n",equal_bool[i]);
			printf("%d\n",great_bool[i]);
			printf("%d\n",less_bool[i]);
			}*/
			int q=0;
			int z=0;
			int value=0;
			char *filerow;
			filerow = (char*)((char*)table_file + sizeof(table_file_header));
			char strValue[MAX_TOK_LEN];
			column = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);

			// printing column names
			column = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			for(z=0;z<num_columns;z++)
			{
				if(z==0)
				{
					if(!(count_bool||sum_bool||avg_bool))
						printf("| %-10s", column->col_name);
				}
				else
				{
					if(!(count_bool||sum_bool||avg_bool))
						printf("| %-11s", column->col_name);
				}
				column++;
			}
			if(!(count_bool||sum_bool||avg_bool)){
				if(z==1)
					printf("\n---------------");
				if(z==2)
					printf("\n--------------------------");
				if(z==3)
					printf("\n--------------------------------------");
				if(z==4)
					printf("\n-------------------------------------------------");
				if(z==5)
					printf("\n-------------------------------------------------------------");}
			bool requiredrow=false;
			// handling AND , OR
			printf("\n");
			while(i<num_rows)
			{
				j=0;
				column = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);							

				char *backupRow = filerow;
				//if(rc) break;  //comment
				bool rowneeded0=false;
				bool rowneeded1=false;
				requiredrow=false;
				while(j<num_columns)
				{										

					int size = (int)(*filerow);
					if(!(stricmp(column->col_name,cols[0])))
						//if(!rc)
					{
						token_list* tempcurr= (token_list*)(colv[0]);
						if(size==0 && (tempcurr->tok_value==K_NULL))
						{								
							rowneeded0=true;
						}
						else if (size>0)
						{
							if(column->col_type==T_INT)
							{														
								if((tempcurr->tok_value==INT_LITERAL))//||(tempcurr->tok_value==K_NOT&&tempcurr->next->tok_value==K_NULL))
								{
									value=0;
									memcpy(&value, filerow+1, size);
									//if(value==atoi(tempcurr->tok_string)) rowneeded0=true;
									if((equal_bool[0]==1)&&(value==atoi(tempcurr->tok_string))) rowneeded0=true;
									if((less_bool[0]==1)&&(value < atoi(tempcurr->tok_string))) rowneeded0=true;
									if((great_bool[0]==1)&&(value > atoi(tempcurr->tok_string))) rowneeded0=true;

								}
								// for handling 'is NULL'
								else if(tempcurr->tok_value==K_NULL)
								{
									if(column->not_null)
									{
										rc=NULL_IS_ASSIGNED;
										tempcurr->tok_value= INVALID;
										return rc;
									}
									else
									{
										value=0;
										memcpy(&value, filerow+1, size);
										//if(value==atoi(tempcurr->tok_string)) rowneeded0=true;
										if((equal_bool[0]==1)&&(value==atoi(tempcurr->tok_string))) rowneeded0=true;
										if((less_bool[0]==1)&&(value < atoi(tempcurr->tok_string))) rowneeded0=true;
										if((great_bool[0]==1)&&(value > atoi(tempcurr->tok_string))) rowneeded0=true;

									}
								}
								
								else
								{
									rc = INCORRECT_DATA_TYPE;
									tempcurr->tok_value = INVALID;
									return rc;
								}
								
							}
							else if(column->col_type==T_CHAR)
							{													
								if(tempcurr->tok_value==STRING_LITERAL)
								{
									memset(strValue, '\0', MAX_TOK_LEN);
									memcpy(strValue, filerow+1, size);
									/*if((equal_bool[0]==1)&&!(stricmp(strValue,tempcurr->tok_string))) rowneeded0=true;
									if((less_bool[0]==1)&&!(stricmp(strValue,tempcurr->tok_string))) rowneeded0=true;
									if((great_bool[0]==1)&&!(stricmp(strValue,tempcurr->tok_string))) rowneeded0=true;*/
									if((equal_bool[0]==1)&&!(stricmp(strValue,tempcurr->tok_string))) rowneeded0=true;
									if((less_bool[0]==1))
									{
										if((stricmp(strValue,tempcurr->tok_string)<0)) 
										{rowneeded0=true;}
									}
									if((great_bool[0]==1))
									{
										if((stricmp(strValue,tempcurr->tok_string)>0))
										{rowneeded0=true;}
									}
								}
								// handling 'is NULL' for Strings 
								else if(tempcurr->tok_value==K_NULL)
								{
									if(column->not_null)
									{
										rc=NULL_IS_ASSIGNED;
										tempcurr->tok_value= INVALID;
										return rc;
									}
									else
									{
										memset(strValue, '\0', MAX_TOK_LEN);
										memcpy(strValue, filerow+1, size);
										/*if((equal_bool[0]==1)&&!(stricmp(strValue,tempcurr->tok_string))) rowneeded0=true;
										if((less_bool[0]==1)&&!(stricmp(strValue,tempcurr->tok_string))) rowneeded0=true;
										if((great_bool[0]==1)&&!(stricmp(strValue,tempcurr->tok_string))) rowneeded0=true;*/
										if((equal_bool[0]==1)&&!(stricmp(strValue,tempcurr->tok_string))) rowneeded0=true;
										if((less_bool[0]==1))
										{
											if((stricmp(strValue,tempcurr->tok_string)<0)) 
											{rowneeded0=true;}
										}
										if((great_bool[0]==1))
										{
											if((stricmp(strValue,tempcurr->tok_string)>0))
											{rowneeded0=true;}
										}
									}
								}

								else
								{
									rc = INCORRECT_DATA_TYPE;
									tempcurr->tok_value = INVALID;
									return rc;
								}
							}
						}
						//q++;  
					}

					if(cols_counter>1)
					{
						if(!(stricmp(column->col_name,cols[1])))
							//if(!rc)
						{
							token_list* tempcurr= (token_list*)(colv[1]);
							if(size==0 && (tempcurr->tok_value==K_NULL))
							{								
								rowneeded1=true;
							}
							else if (size>0)
							{
								if(column->col_type==T_INT)
								{														
									if(tempcurr->tok_value==INT_LITERAL)
									{
										value=0;
										memcpy(&value, filerow+1, size);
										//if(value==atoi(tempcurr->tok_string)) rowneeded1=true;
										if((equal_bool[1]==1)&&(value==atoi(tempcurr->tok_string))) rowneeded1=true;
										if((less_bool[1]==1)&&(value < atoi(tempcurr->tok_string))) rowneeded1=true;
										if((great_bool[1]==1)&&(value > atoi(tempcurr->tok_string))) rowneeded1=true;
									}

									else if(tempcurr->tok_value==K_NULL)
									{
										if(column->not_null)
										{
											rc=NULL_IS_ASSIGNED;
											tempcurr->tok_value= INVALID;
											return rc;
										}
										else
										{
											value=0;
											memcpy(&value, filerow+1, size);
											//if(value==atoi(tempcurr->tok_string)) rowneeded1=true;
											if((equal_bool[1]==1)&&(value==atoi(tempcurr->tok_string))) rowneeded1=true;
											if((less_bool[1]==1)&&(value < atoi(tempcurr->tok_string))) rowneeded1=true;
											if((great_bool[1]==1)&&(value > atoi(tempcurr->tok_string))) rowneeded1=true;
										}
									}

									else
									{
										rc = INCORRECT_DATA_TYPE;
										tempcurr->tok_value = INVALID;
										return rc;
									}
									

								}
								else if(column->col_type==T_CHAR)
								{													
									if(tempcurr->tok_value==STRING_LITERAL)
									{
										memset(strValue, '\0', MAX_TOK_LEN);
										memcpy(strValue, filerow+1, size);
										//if(!(stricmp(strValue,tempcurr->tok_string))) rowneeded1=true;
										if((equal_bool[1]==1)&&!(stricmp(strValue,tempcurr->tok_string))) rowneeded1=true;
										if((less_bool[1]==1))
										{
											if((stricmp(strValue,tempcurr->tok_string)<0)) 
											{rowneeded1=true;}
										}
										if((great_bool[1]==1))
										{
											if((stricmp(strValue,tempcurr->tok_string)>0))
											{rowneeded1=true;}
										}
									}

									else if(tempcurr->tok_value==K_NULL)
									{
										
										
										if(column->not_null)
										{
											rc=NULL_IS_ASSIGNED;
											tempcurr->tok_value= INVALID;
											return rc;
										}
										else
										{
											memset(strValue, '\0', MAX_TOK_LEN);
											memcpy(strValue, filerow+1, size);
											//if(!(stricmp(strValue,tempcurr->tok_string))) rowneeded1=true;
											if((equal_bool[1]==1)&&!(stricmp(strValue,tempcurr->tok_string))) rowneeded1=true;
											if((less_bool[1]==1))
											{
												if((stricmp(strValue,tempcurr->tok_string)<0)) 
												{rowneeded1=true;}
											}
											if((great_bool[1]==1))
											{
												if((stricmp(strValue,tempcurr->tok_string)>0))
												{rowneeded1=true;}
											}
										}
									}

									else
									{
										rc = INCORRECT_DATA_TYPE;
										tempcurr->tok_value = INVALID;
										return rc;
									}
								}
							}
							//q++;  
						}
					}



					filerow = filerow+1+column->col_len;
					column++;			
					j++;
				}

				if(rowneeded0==true && cols_counter==1) requiredrow=true;
				if(cols_counter==2)
				{
					if(!(stricmp(oprtr[0],"and")))
					{
						if (rowneeded0==true &&  rowneeded1==true) requiredrow=true;
					}
					else if(!(stricmp(oprtr[0],"or")))
					{
						if (rowneeded0==true ||  rowneeded1==true) requiredrow=true;
					}

				}
				column = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);							

				filerow= backupRow;
				j=0;
				if(requiredrow){
					count_counter++;
					while(j<num_columns)
					{										

						int size = (int)(*filerow);
						if(!rc)
						{

							if(size==0)
							{								
								if(!(count_bool||sum_bool||avg_bool))printf("| ");
								if(!(count_bool||sum_bool||avg_bool))printf("%10s","-");
								/*if(j!=0)
								printf("\t");*/
							}
							else
							{
								if(column->col_type==T_INT)
								{				
									if(!(count_bool||sum_bool||avg_bool))printf("| ");
									value=0;
									memcpy(&value, filerow+1, size);
									/*if(j!=0)
									{printf("\t");}*/
									if(!(count_bool||sum_bool||avg_bool))printf("%10d",value); 

								}
								else if(column->col_type==T_CHAR)
								{			
									if(!(count_bool||sum_bool||avg_bool))printf("| ");
									memset(strValue, '\0', MAX_TOK_LEN);
									memcpy(strValue, filerow+1, size);
									/*if(j!=0)
									{printf("\t");}*/
									if(!(count_bool||sum_bool||avg_bool))printf("%-10s",strValue); 
								}
							}
							//q++;
						}


						filerow = filerow+1+column->col_len;
						column++;			
						j++;
					}
				}
				else
				{
					filerow=filerow+table_file->record_size;
				}
				i++;
				if(!(count_bool||sum_bool||avg_bool))
				{
					if (requiredrow) printf("\n");
				}

			}
			if(count_bool==true)
			{
				printf("The Count is: %d",count_counter);
				printf("\n");
			}
			if(sum_bool==true)
			{
				rc=INVALID_AGG_FUNCT_PARAM;
				cur->tok_value=INVALID;
				return rc;
			}
			if(avg_bool==true)
			{
				rc=INVALID_AGG_FUNCT_PARAM;
				cur->tok_value=INVALID;
				return rc;
			}


			return rc;
		}

		if(cols_bool && where_bool)
		{
			int q=0;
			int z=0,b=0;
			int value=0;
			char *filerow;
			filerow = (char*)((char*)table_file + sizeof(table_file_header));
			char strValue[MAX_TOK_LEN];
			column = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);

			// printing column names
			for(z=0;z<num_columns;z++)
			{
				if(column->col_id==col_ids[z])
				{
					if(z==0)
					{
						if(!(count_bool||sum_bool||avg_bool))
							printf("| %-10s", column->col_name);
					}
					else
					{
						if(!(count_bool||sum_bool||avg_bool))
							printf("| %-11s", column->col_name);
					}
					//b++;
				}

				column++;
			}
			if(!(count_bool||sum_bool||avg_bool)){
				if(z==1)
					printf("\n---------------");
				if(z==2)
					printf("\n--------------------------");
				if(z==3)
					printf("\n--------------------------------------");
				if(z==4)
					printf("\n-------------------------------------------------");
				if(z==5)
					printf("\n-------------------------------------------------------------");}
			if(!(count_bool||sum_bool||avg_bool))printf("\n");
			// handling AND , OR
			bool requiredrow=false;
			while(i<num_rows)
			{
				j=0;
				column = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);							

				char *backupRow = filerow;
				//if(rc) break;  //comment
				bool rowneeded0=false;
				bool rowneeded1=false;
				requiredrow=false;
				while(j<num_columns)
				{										

					int size = (int)(*filerow);
					if(!(stricmp(column->col_name,cols[0])))
						//if(!rc)
					{
						token_list* tempcurr= (token_list*)(colv[0]);
						if(size==0 && (tempcurr->tok_value==K_NULL))
						{								
							rowneeded0=true;
						}
						else if (size>0)
						{
							if(column->col_type==T_INT)
							{														
								if(tempcurr->tok_value==INT_LITERAL)
								{	
									value=0;
									memcpy(&value, filerow+1, size);
									/*if(value==atoi(tempcurr->tok_string)) rowneeded0=true;*/
									if((equal_bool[0]==1)&&(value==atoi(tempcurr->tok_string))) rowneeded0=true;
									if((less_bool[0]==1)&&(value < atoi(tempcurr->tok_string))) rowneeded0=true;
									if((great_bool[0]==1)&&(value > atoi(tempcurr->tok_string))) rowneeded0=true;
								}

								else if(tempcurr->tok_value==K_NULL)
								{
									if(column->not_null)
									{
										rc=NULL_IS_ASSIGNED;
										tempcurr->tok_value= INVALID;
										return rc;
									}
									else
									{
										value=0;
										memcpy(&value, filerow+1, size);
										//if(value==atoi(tempcurr->tok_string)) rowneeded0=true;
										if((equal_bool[0]==1)&&(value==atoi(tempcurr->tok_string))) rowneeded0=true;
										if((less_bool[0]==1)&&(value < atoi(tempcurr->tok_string))) rowneeded0=true;
										if((great_bool[0]==1)&&(value > atoi(tempcurr->tok_string))) rowneeded0=true;

									}
								}

								else
								{
									rc = INCORRECT_DATA_TYPE;
									tempcurr->tok_value = INVALID;
									return rc;
								}
							}

							else if(column->col_type==T_CHAR)
							{													
								if(tempcurr->tok_value==STRING_LITERAL)
								{
									memset(strValue, '\0', MAX_TOK_LEN);
									memcpy(strValue, filerow+1, size);
									/*if(!(stricmp(strValue,tempcurr->tok_string))) rowneeded0=true;*/
									if((equal_bool[0]==1)&&!(stricmp(strValue,tempcurr->tok_string))) rowneeded0=true;
									if((less_bool[0]==1))
									{
										if((stricmp(strValue,tempcurr->tok_string)<0)) 
										{rowneeded0=true;}
									}
									if((great_bool[0]==1))
									{
										if((stricmp(strValue,tempcurr->tok_string)>0))
										{rowneeded0=true;}
									}
								}
								else if(tempcurr->tok_value==K_NULL)
								{
									if(column->not_null)
									{
										rc=NULL_IS_ASSIGNED;
										tempcurr->tok_value= INVALID;
										return rc;
									}
									else
									{
										memset(strValue, '\0', MAX_TOK_LEN);
										memcpy(strValue, filerow+1, size);
										/*if((equal_bool[0]==1)&&!(stricmp(strValue,tempcurr->tok_string))) rowneeded0=true;
										if((less_bool[0]==1)&&!(stricmp(strValue,tempcurr->tok_string))) rowneeded0=true;
										if((great_bool[0]==1)&&!(stricmp(strValue,tempcurr->tok_string))) rowneeded0=true;*/
										if((equal_bool[0]==1)&&!(stricmp(strValue,tempcurr->tok_string))) rowneeded0=true;
										if((less_bool[0]==1))
										{
											if((stricmp(strValue,tempcurr->tok_string)<0)) 
											{rowneeded0=true;}
										}
										if((great_bool[0]==1))
										{
											if((stricmp(strValue,tempcurr->tok_string)>0))
											{rowneeded0=true;}
										}
									}
								}

								else
								{
									rc = INCORRECT_DATA_TYPE;
									tempcurr->tok_value = INVALID;
									return rc;
								}
							}
						}
						//q++;  
					}

					if(cols_counter>1)
					{
						if(!(stricmp(column->col_name,cols[1])))
							//if(!rc)
						{
							token_list* tempcurr= (token_list*)(colv[1]);
							if(size==0 && (tempcurr->tok_value==K_NULL))
							{								
								rowneeded1=true;
							}
							else if (size>0)
							{
								if(column->col_type==T_INT)
								{														
									if(tempcurr->tok_value==INT_LITERAL)	
									{
										value=0;
										memcpy(&value, filerow+1, size);
										/*int value2=atoi(tempcurr->tok_string);//(int)(tempcurr->tok_string);
										if(value==value2) rowneeded1=true;*/
										if((equal_bool[1]==1)&&(value==atoi(tempcurr->tok_string))) rowneeded1=true;
										if((less_bool[1]==1)&&(value < atoi(tempcurr->tok_string))) rowneeded1=true;
										if((great_bool[1]==1)&&(value > atoi(tempcurr->tok_string))) rowneeded1=true;
									}
									else if(tempcurr->tok_value==K_NULL)
									{
										if(column->not_null)
										{
											rc=NULL_IS_ASSIGNED;
											tempcurr->tok_value= INVALID;
											return rc;
										}
										else
										{
											value=0;
											memcpy(&value, filerow+1, size);
											//if(value==atoi(tempcurr->tok_string)) rowneeded1=true;
											if((equal_bool[1]==1)&&(value==atoi(tempcurr->tok_string))) rowneeded1=true;
											if((less_bool[1]==1)&&(value < atoi(tempcurr->tok_string))) rowneeded1=true;
											if((great_bool[1]==1)&&(value > atoi(tempcurr->tok_string))) rowneeded1=true;
										}
									}

									else
									{
										rc = INCORRECT_DATA_TYPE;
										tempcurr->tok_value = INVALID;
										return rc;
									}

								}
								else if(column->col_type==T_CHAR)
								{													
									if(tempcurr->tok_value==STRING_LITERAL)
									{
										memset(strValue, '\0', MAX_TOK_LEN);
										memcpy(strValue, filerow+1, size);
										/*if(!(stricmp(strValue,tempcurr->tok_string))) rowneeded1=true;*/
										if((equal_bool[1]==1)&&!(stricmp(strValue,tempcurr->tok_string))) rowneeded1=true;
										if((less_bool[1]==1))
										{
											if((stricmp(strValue,tempcurr->tok_string)<0)) 
											{rowneeded1=true;}
										}
										if((great_bool[1]==1))
										{
											if((stricmp(strValue,tempcurr->tok_string)>0))
											{rowneeded1=true;}
										}
									}
									else if(tempcurr->tok_value==K_NULL)
									{
										
										
										if(column->not_null)
										{
											rc=NULL_IS_ASSIGNED;
											tempcurr->tok_value= INVALID;
											return rc;
										}
										else
										{
											memset(strValue, '\0', MAX_TOK_LEN);
											memcpy(strValue, filerow+1, size);
											//if(!(stricmp(strValue,tempcurr->tok_string))) rowneeded1=true;
											if((equal_bool[1]==1)&&!(stricmp(strValue,tempcurr->tok_string))) rowneeded1=true;
											if((less_bool[1]==1))
											{
												if((stricmp(strValue,tempcurr->tok_string)<0)) 
												{rowneeded1=true;}
											}
											if((great_bool[1]==1))
											{
												if((stricmp(strValue,tempcurr->tok_string)>0))
												{rowneeded1=true;}
											}
										}
									}

									else
									{
										rc = INCORRECT_DATA_TYPE;
										tempcurr->tok_value = INVALID;
										return rc;
									}

								}
							}
							//q++;  
						}
					}



					filerow = filerow+1+column->col_len;
					column++;			
					j++;
				}

				if(rowneeded0==true && cols_counter==1) requiredrow=true;
				if(cols_counter==2)
				{
					if(!(stricmp(oprtr[0],"and")))
					{
						if (rowneeded0==true &&  rowneeded1==true) requiredrow=true;
					}
					else if(!(stricmp(oprtr[0],"or")))
					{
						if (rowneeded0==true ||  rowneeded1==true) requiredrow=true;
					}

				}
				column = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);							

				filerow= backupRow;
				j=0;
				int p=0;
				if(requiredrow){
					count_counter++;
					while(j<num_columns)
					{										
						if(column->col_id==col_ids[p]&&col_ids[p]!=16)
						{
							int size = (int)(*filerow);
							if(!rc)
							{

								if(size==0)
								{								
									if(!(count_bool||sum_bool||avg_bool))printf("| ");
									if(!(count_bool||sum_bool||avg_bool))printf("%10s","-");
									null_counter--;
									/*if(j!=0)
									printf("\t");*/
								}
								else
								{
									if(column->col_type==T_INT)
									{				
										if(!(count_bool||sum_bool||avg_bool))printf("| ");
										value=0;
										memcpy(&value, filerow+1, size);
										/*if(j!=0)
										{printf("\t");}*/
										if(!(count_bool||sum_bool||avg_bool))printf("%10d",value);
										sum=sum+value;

									}
									else if(column->col_type==T_CHAR)
									{			
										if(!(count_bool||sum_bool||avg_bool))printf("| ");
										memset(strValue, '\0', MAX_TOK_LEN);
										memcpy(strValue, filerow+1, size);
										/*if(j!=0)
										{printf("\t");}*/
										if(!(count_bool||sum_bool||avg_bool))printf("%-10s",strValue); 
									}
								}
								//q++;
							}

						}p++;
						filerow = filerow+1+column->col_len;
						column++;			
						j++;
					}
				}
				else
				{
					filerow=filerow+table_file->record_size;
				}
				i++;
				if(!(count_bool||sum_bool||avg_bool))
				{if (requiredrow)printf("\n");}
			}
			count_counter = count_counter-null_counter;
			if(count_bool)printf("The Count is: %d",count_counter);
			if(sum_bool)printf("The Sum is: %d",sum);
			if(avg_bool)printf("The Avg is: %f",(float)sum/count_counter);
			printf("\n");
			count_bool=false;sum_bool=false;avg_bool=false;

			return rc;
		}

		if(star_bool)
		{
			int value=0;
			int z,b;
			char *filerow;
			filerow = (char*)((char*)table_file + sizeof(table_file_header));
			char strValue[MAX_TOK_LEN];
			// picking rows and writing them to console
			printf("\n");

			// printing column names
			column = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			for(z=0;z<num_columns;z++)
			{
				if(z==0)
				{	
					if(!(count_bool||sum_bool||avg_bool))
						printf("| %-10s", column->col_name);
				}
				else
				{	
					if(!(count_bool||sum_bool||avg_bool))
						printf("| %-11s", column->col_name);
				}
				column++;
			}
			if(!(count_bool||sum_bool||avg_bool)){
				if(z==1)
					printf("\n---------------");
				if(z==2)
					printf("\n--------------------------");
				if(z==3)
					printf("\n--------------------------------------");
				if(z==4)
					printf("\n-------------------------------------------------");
				if(z==5)
					printf("\n-------------------------------------------------------------");}



			while(i<num_rows)
			{
				j=0;
				column = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);							
				if(!(count_bool||sum_bool||avg_bool))printf("\n");
				while(j<num_columns)
				{														
					int size = (int)(*filerow);
					if(!rc)
					{

						if(size==0)
						{								
							if(!(count_bool||sum_bool||avg_bool))printf("| ");
							if(!(count_bool||sum_bool||avg_bool))printf("%10s","-");
							//null_counter++;
							/*if(j!=0)
							printf("\t");*/
						}
						else
						{
							if(column->col_type==T_INT)
							{				
								if(!(count_bool||sum_bool||avg_bool))printf("| ");
								value=0;
								memcpy(&value, filerow+1, size);
								/*if(j!=0)
								{printf("\t");}*/
								if(!(count_bool||sum_bool||avg_bool))printf("%10d",value); 
								sum=sum+value;

							}
							else if(column->col_type==T_CHAR)
							{			
								if(!(count_bool||sum_bool||avg_bool))printf("| ");
								memset(strValue, '\0', MAX_TOK_LEN);
								memcpy(strValue, filerow+1, size);
								/*if(j!=0)
								{printf("\t");}*/
								if(!(count_bool||sum_bool||avg_bool))printf("%-10s",strValue); 
							}
						}
					}
					filerow = filerow+1+column->col_len;
					column++;			
					j++;

				}
				i++;
				count_counter++;
			}
			count_counter = count_counter;
			if(count_bool)printf("The Count is: %d",count_counter);
			if(sum_bool)
			{
				rc=INVALID_AGG_FUNCT_PARAM;
				cur->tok_value=INVALID;
				return rc;
			}
			if(avg_bool)
			{
				rc=INVALID_AGG_FUNCT_PARAM;
				cur->tok_value=INVALID;
				return rc;
			}
			printf("\n");
			count_bool=false;sum_bool=false;avg_bool=false;
		}

		if(cols_bool)
		{

			int value=0;
			int z=0,b=0;
			char *filerow;
			filerow = (char*)((char*)table_file + sizeof(table_file_header));
			char strValue[MAX_TOK_LEN];
			// picking rows and writing them to console
			printf("\n");

			// printing column names
			column = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			int c=0;
			for(z=0;z<num_columns;z++)
			{
				if(column->col_id==col_ids[z])
				{
					if(b==0)
					{if(!(count_bool||sum_bool||avg_bool))printf("| %-10s", column->col_name);}
					else
					{if(!(count_bool||sum_bool||avg_bool))printf("| %-11s", column->col_name);}
					//b++;
				}

				column++;
			}
			if(!(count_bool||sum_bool||avg_bool)){
				if(z==1)
					printf("\n---------------");
				if(z==2)
					printf("\n--------------------------");
				if(z==3)
					printf("\n--------------------------------------");
				if(z==4)
					printf("\n-------------------------------------------------");
				if(z==5)
					printf("\n-------------------------------------------------------------");}


			int p=0;
			//printf("%d",num_rows);
			while(i<num_rows)
			{
				j=0;
				p=0;
				column = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);							
				if(!(count_bool||sum_bool||avg_bool))printf("\n");
				while(j<num_columns)
				{
					if(column->col_id==col_ids[p]&&col_ids[p]!=16)
					{														
						int size = (int)(*filerow);
						if(!rc)
						{

							if(size==0)
							{								
								if(!(count_bool||sum_bool||avg_bool))printf("| ");
								if(!(count_bool||sum_bool||avg_bool))printf("%10s","-");
								null_counter++;
								/*if(j!=0)
								printf("\t");*/
							}
							else
							{
								if(column->col_type==T_INT)
								{				
									if(!(count_bool||sum_bool||avg_bool))printf("| ");
									value=0;
									memcpy(&value, filerow+1, size);
									/*if(j!=0)
									{printf("\t");}*/
									if(!(count_bool||sum_bool||avg_bool))printf("%10d",value); 
									sum=sum+value;

								}
								else if(column->col_type==T_CHAR)
								{			
									if(!(count_bool||sum_bool||avg_bool))printf("| ");
									memset(strValue, '\0', MAX_TOK_LEN);
									memcpy(strValue, filerow+1, size);
									/*if(j!=0)
									{printf("\t");}*/
									if(!(count_bool||sum_bool||avg_bool))printf("%-10s",strValue); 
								}
							}
						}



					}p++;		

					filerow = filerow+1+column->col_len;
					column++;
					j++;



				}
				i++;
				count_counter++;
			}

			count_counter = count_counter-null_counter;
			if(count_bool)printf("The Count is: %d",count_counter);
			if(sum_bool)printf("The Sum is: %d",sum);
			if(avg_bool)printf("The Avg is: %f",(float)sum/count_counter);
			printf("\n");
			count_bool=false;sum_bool=false;avg_bool=false;

		}
	}


	return rc;
}

int sem_update(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	token_list *temp_cur;
	char filename[MAX_IDENT_LEN+4]; 
	char *data_value;
	char *cols;
	char *colv;
	bool equal_flag=false;
	bool less_flag=false;
	bool great_flag=false;
	tpd_entry *tab_entry;
	bool where_flag=false;
	cd_entry *column;
	cur = t_list;
	temp_cur = t_list;
	FILE *fhandle = NULL;
	struct _stat file_stat;

	// handling the table

	if ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
		return rc;
	}

	else
	{
		if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
		{
			rc = TABLE_NOT_EXIST;
			cur->tok_value = INVALID;
			return rc;
		}

		//opening the tablefile

		if(!rc)
		{
			strcpy(filename,cur->tok_string);
			strcat(filename,".tab");
			fhandle = fopen(filename, "rbc");
			_fstat(_fileno(fhandle), &file_stat);
			//printf("%s file size is %d\n",filename,file_stat.st_size);
			table_file = (table_file_header*)calloc(1, file_stat.st_size);

			if (!table_file)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				fread(table_file, file_stat.st_size, 1, fhandle);
				//fflush(fhandle);
				fclose(fhandle);
				if (table_file->file_size != file_stat.st_size)
				{
					rc = TABLEFILE_CORRUPTION;
				}

			}
		}
	}


	// handling set

	cur = cur->next;
	if (cur->tok_value != K_SET)
	{
		//Error
		rc = INVALID_TABLE_DEFINITION;
		cur->tok_value = INVALID;
		return rc;
	}



	cur = cur->next;

	if (!rc && (cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_COLUMN_NAME;
		cur->tok_value = INVALID;
		return rc;
	}

	// finding the column id(s) that are to be updated

	int num_columns,j=0,col_id=0;
	column = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
	num_columns=tab_entry->num_columns;

	while(j<num_columns)
	{														
		if(!(stricmp(column->col_name,cur->tok_string)))
		{
			col_id=column->col_id;
			//printf("\n%d",col_id);
			break;
		}
		column++;			
		j++;
	}


	column = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
	// continue parsing and handle 'equals'  and 'the value to be updated'

	cur = cur->next;
	if(!rc && cur->tok_value!=S_EQUAL)
	{
		//Error
		rc = INVALID_TABLE_DEFINITION;
		cur->tok_value = INVALID;
		return rc;
	}


	int i=0;
	int num_rows=table_file->num_records;
	int offset=0, si=sizeof(int),char_size;
	int value;
	char* filerow;
	filerow = (char *)calloc(table_file->num_records, table_file->record_size); 
	memset(filerow,'\0',table_file->num_records*table_file->record_size);

	// get the column pointer to the column that matches the column id where the value should be inserted
	while(!rc)
	{
		if(column->col_id==col_id)
			break;
		else
			column++;
	}


	//printf("\n%d",column->col_id);
	do
	{
		if(temp_cur->tok_value==K_WHERE)
		{
			where_flag=true;
			break;
		}

		temp_cur=temp_cur->next;

	}while(temp_cur->tok_value!=EOC);



	if(!where_flag)
	{
		cur = cur->next;
		if (cur->tok_value == EOC)
		{
			
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
			return rc;
		}
		filerow = (char*)((char*)table_file + sizeof(table_file_header));

		while(i<num_rows)
		{
			j=0;
			column = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);							
			while(j<num_columns)
			{
				//handling integer values
				if(col_id == column->col_id)
				{
					
					if (column->col_type == T_INT && cur->tok_value==K_NULL && column->not_null)
					{
							rc = NULL_IS_ASSIGNED;
							cur->tok_value = INVALID;
							return rc;
			
					}
					
					
					else if(column->col_type == T_INT && cur->tok_value != INT_LITERAL && cur->tok_value!=K_NULL)
					{
						rc = INCORRECT_DATA_TYPE;
						cur->tok_value = INVALID;
						break;
					}

					
					else if(column->col_type == T_INT && cur->tok_value == INT_LITERAL)
					{

						int nullSize = (int) *filerow;
						if(nullSize==0)
						{
							nullSize = sizeof(int);
							memcpy(filerow, &nullSize, 1);
						}
						value = atoi(cur->tok_string);
						memcpy(filerow+1, &value,si);
					}

					//handling character values

					else if (column->col_type == T_CHAR && cur->tok_value==K_NULL && column->not_null)
					{
							rc = NULL_IS_ASSIGNED;
							cur->tok_value = INVALID;
							return rc;
						
					}

					
					else if ((column->col_type == T_CHAR)&&(cur->tok_value != STRING_LITERAL && cur->tok_value!=K_NULL))
					{
						rc = INCORRECT_DATA_TYPE;
						cur->tok_value = INVALID;
						break;
					}
					
					else if ((column->col_type == T_CHAR)&&(cur->tok_value == STRING_LITERAL))
					{

						int nullSize = (int) *filerow;
						if(nullSize==0)
						{
							nullSize = sizeof(int);
							memcpy(filerow, &nullSize, 1);
						}


						memset(filerow+1, '\0', column->col_len); 
						memcpy(filerow+1,cur->tok_string,column->col_len);

					}
					else if(cur->tok_value==K_NULL)
					{
						int nullSize = 0;
						memcpy(filerow, &nullSize, 1);

					}
					
					
				}
				j=j+1;
				filerow = filerow+1+column->col_len;
				column++;
			}
			i=i+1;


		}
		fhandle = NULL;
		fhandle = fopen(filename, "wbc");
		fwrite(table_file, table_file -> file_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);

	}
	else
	{
		if(where_flag)
		{
			char *checking_row;
			checking_row = (char*)((char*)table_file + sizeof(table_file_header));
			cur=cur->next;
			if (cur->tok_value == EOC)
			{
			
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
				return rc;
			}
			token_list* data_value= (token_list*)(cur);
			
			//data_value=cur->tok_string;
			cur=cur->next;
			if(cur->tok_value!=K_WHERE && cur->next->tok_value==EOC)
			{
				rc = INVALID_STATEMENT;
				return rc;

			}
			else
			{
				cur = cur->next;
				if(cur->tok_value==EOC)
				{
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
					return rc;
				}
				if(cur->tok_value!=IDENT)
				{
					rc = INVALID_STATEMENT;
					return rc;

				}
				else
				{
					cols=cur->tok_string;
					cur=cur->next;
					if(cur->tok_value==EOC)
					{
						rc = INVALID_STATEMENT;
						cur->tok_value = INVALID;
						return rc;
					}
					
					/*if ((cur->tok_value!=S_EQUAL) || (cur->tok_value!=S_GREATER) || (cur->tok_value!=S_LESS))
					{
			
						rc = INVALID_STATEMENT;
						cur->tok_value = INVALID;
						return rc;
					}*/
					if((cur->tok_value==S_EQUAL) || (cur->tok_value==S_GREATER) || (cur->tok_value==S_LESS))
					{
						if(cur->tok_value==S_EQUAL) equal_flag=true;
						if(cur->tok_value==S_GREATER) great_flag=true;
						if(cur->tok_value==S_LESS) less_flag=true;
						cur=cur->next;
						colv=cur->tok_string;
						/*if(cur->tok_value!=EOC)
						{
							rc = INVALID_STATEMENT;
							cur->tok_value = INVALID;
							return rc;
						}*/
					}
				}
			}

			bool requiredrow=false;
			char strValue[MAX_TOK_LEN];
			num_columns=tab_entry->num_columns;
			int i=0,j=0;
			int value=0;
			while(i<num_rows)
			{
				j=0;
				column = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);							

				char *backupRow = checking_row;
				bool rowneeded0=false;
				requiredrow=false;
				while(j<num_columns)
				{										

					int size = (int)(*checking_row);
					if(!(stricmp(column->col_name,cols)))
					{
						token_list* tempcurr= (token_list*)(colv);
						if(size==0 && (tempcurr->tok_value==K_NULL))
						{								
							rowneeded0=true;
						}
						else if (size>0)
						{
							if(column->col_type==T_INT)
							{														
								
								
								/*if(tempcurr->tok_value==K_NULL && column->not_null)
								{
										rc = NULL_IS_ASSIGNED;
										cur->tok_value = INVALID;
										return rc;
								}*/
								
								if(tempcurr->tok_value == INT_LITERAL)
								{
									value=0;
									memcpy(&value, checking_row+1, size);
									if(equal_flag &&(value==atoi(tempcurr->tok_string))) rowneeded0=true;
									if(less_flag &&(value < atoi(tempcurr->tok_string))) rowneeded0=true;
									if(great_flag &&(value > atoi(tempcurr->tok_string))) rowneeded0=true;
								}
								else
								{
									rc = INCORRECT_DATA_TYPE;
									tempcurr->tok_value = INVALID;
									return rc;

								}
							}
							else if(column->col_type==T_CHAR)
							{													
								
								/*if(tempcurr->tok_value==K_NULL && column->not_null)
								{
										rc = NULL_IS_ASSIGNED;
										cur->tok_value = INVALID;
										return rc;
								}*/

								if(tempcurr->tok_value==STRING_LITERAL)
								{
									memset(strValue, '\0', MAX_TOK_LEN);
									memcpy(strValue, checking_row+1, size);
									if(equal_flag &&!(stricmp(strValue,tempcurr->tok_string))) rowneeded0=true;
									if(less_flag)
									{
										if((stricmp(strValue,tempcurr->tok_string)<0)) 
										{rowneeded0=true;}
									}
									if(great_flag)
									{
										if((stricmp(strValue,tempcurr->tok_string)>0))
										{rowneeded0=true;}
									}
								}
								else
								{
									rc = INCORRECT_DATA_TYPE;
									tempcurr->tok_value = INVALID;
									return rc;
								}
							}
						}
						//q++;  
					}


					checking_row = checking_row+1+column->col_len;
					column++;			
					j++;
				}

				if(rowneeded0==true) requiredrow=true;
				column = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);							

				checking_row= backupRow;
				j=0;

				if(requiredrow)
				{
					j=0;
					column = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);							
					while(j<num_columns)
					{

						if(col_id == column->col_id)
						{
							
							if(column->col_type == T_INT && data_value->tok_value == K_NULL && column->not_null)
							{
								rc = NULL_IS_ASSIGNED;
								cur->tok_value = INVALID;
								return rc;
							}
							else if(column->col_type == T_INT && data_value->tok_value != INT_LITERAL && data_value->tok_value!=K_NULL)
							{
								rc = INCORRECT_DATA_TYPE;
								cur->tok_value = INVALID;
								break;
							}

							else if(column->col_type == T_INT && data_value->tok_value == INT_LITERAL)
							{

								int nullSize = (int) *checking_row;
								if(nullSize==0)
								{
									nullSize = sizeof(int);
									memcpy(checking_row, &nullSize, 1);
								}
								value = atoi(data_value->tok_string);
								memcpy(checking_row+1, &value,si);
							}

							//handling character values
							else if(column->col_type == T_CHAR && data_value->tok_value == K_NULL && column->not_null)
							{
								rc = NULL_IS_ASSIGNED;
								cur->tok_value = INVALID;
								return rc;
							}

							else if ((column->col_type == T_CHAR)&&(data_value->tok_value != STRING_LITERAL)&&(data_value->tok_value!=K_NULL))
							{
								rc = INCORRECT_DATA_TYPE;
								data_value->tok_value = INVALID;
								break;
							}

							else if ((column->col_type == T_CHAR)&&(data_value->tok_value == STRING_LITERAL))
							{

								int nullSize = (int) *checking_row;
								if(nullSize==0)
								{
									nullSize = sizeof(int);
									memcpy(checking_row, &nullSize, 1);
								}


								memset(checking_row+1, '\0', column->col_len); 
								memcpy(checking_row+1,data_value->tok_string,column->col_len);

							}
							else if(data_value->tok_value==K_NULL)
							{
								int nullSize = 0;
								memcpy(checking_row, &nullSize, 1);

							}
						}
						j=j+1;
						checking_row = checking_row+1+column->col_len;
						column++;
					}

				}	

				else
				{
					checking_row=checking_row+table_file->record_size;
				}
				i++;

			}

			fhandle = NULL;
			fhandle = fopen(filename, "wbc");
			fwrite(table_file, table_file -> file_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);

		}

	}
	if(rc==0)
	{
		int a=0;
		a=logging();

		if(a==0)
			return rc;
	}
	return rc;
}

int sem_delete(token_list *t_list)
{
	int rc = 0;
	int old_filesize=0,new_filesize=0;
	int num_rows=0;
	int num_columns=0;
	token_list *cur;
	token_list *temp_cur;
	char filename[MAX_IDENT_LEN+4]; 
	tpd_entry *tab_entry;
	bool where_flag=false;
	bool equal_flag=false;
	bool less_flag=false;
	bool great_flag=false;
	char *cols;
	char *colv;
	cd_entry *column;
	cur = t_list;
	temp_cur = t_list;
	FILE *fhandle = NULL;
	struct _stat file_stat;
	table_file_header *temp_table_file;


	if ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}

	else
	{
		if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
		{
			rc = TABLE_NOT_EXIST;
			cur->tok_value = INVALID;
		}


		//opening the tablefile

		if(!rc)
		{
			strcpy(filename,cur->tok_string);
			strcat(filename,".tab");
			fhandle = fopen(filename, "rbc");
			_fstat(_fileno(fhandle), &file_stat);
			table_file = (table_file_header*)calloc(1, file_stat.st_size);
			temp_table_file = (table_file_header*)calloc(1, file_stat.st_size);
			num_columns=tab_entry->num_columns;

			if (!table_file)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				fread(table_file, file_stat.st_size, 1, fhandle);
				old_filesize=table_file->file_size; // for logging
				//fread(temp_table_file, sizeof(table_file_header), 1, fhandle);
				memcpy(temp_table_file, table_file,sizeof(table_file_header));
				num_rows=table_file->num_records;
				temp_table_file->num_records=0;
				temp_table_file->file_size=sizeof(table_file_header);

				if (table_file->file_size != file_stat.st_size)
				{
					rc = TABLEFILE_CORRUPTION;
				}

			}
			fclose(fhandle);
		}
	}

	do
	{
		if(temp_cur->tok_value==K_WHERE)
		{
			where_flag=true;
		}
		temp_cur=temp_cur->next;
	}while(temp_cur->tok_value!=EOC);

	if(!where_flag)
	{
		//writing only the header into the file
		fhandle = NULL;
		fhandle = fopen(filename, "wbc");
		table_file->num_records=0;
		table_file->file_size=sizeof(table_file_header);
		new_filesize=table_file->file_size;
		fwrite(table_file, sizeof(table_file_header), 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
		printf("The number of records are: %d",table_file->num_records);
		if(old_filesize!=new_filesize)
		{
			int a=0;
			a=logging();

			if(a==0)
			return rc;
		}
	}
	else
	{
		if(where_flag)
		{
			char *checking_row;
			checking_row = (char*)((char*)table_file + sizeof(table_file_header));
			cur=cur->next;
			if(cur->tok_value!=K_WHERE)
			{
				rc = INVALID_STATEMENT;
				return rc;

			}
			else
			{
				cur = cur->next;
				if(cur->tok_value!=IDENT)
				{
					rc = INVALID_STATEMENT;
					return rc;

				}
				else
				{
					cols=cur->tok_string;
					cur=cur->next;
					if((cur->tok_value==S_EQUAL) || (cur->tok_value==S_GREATER) || (cur->tok_value==S_LESS))
					{
						if(cur->tok_value==S_EQUAL) equal_flag=true;
						if(cur->tok_value==S_GREATER) great_flag=true;
						if(cur->tok_value==S_LESS) less_flag=true;
						cur=cur->next;
						colv=cur->tok_string;
					}
				}
			}

			bool requiredrow=false;
			char strValue[MAX_TOK_LEN];
			num_columns=tab_entry->num_columns;
			int i=0,j=0;
			int offset=sizeof(table_file_header);
			int value=0;
			while(i<num_rows)
			{
				j=0;
				column = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);							

				char *backupRow = checking_row;
				bool rowneeded0=false;
				requiredrow=false;
				while(j<num_columns)
				{										

					int size = (int)(*checking_row);
					if(!(stricmp(column->col_name,cols)))
					{
						token_list* tempcurr= (token_list*)(colv);
						if(size==0 && (tempcurr->tok_value==K_NULL))
						{								
							rowneeded0=true;
						}
						else if (size>0)
						{
							if(column->col_type==T_INT)
							{														
								if(tempcurr->tok_value==INT_LITERAL)
								{
									value=0;
									memcpy(&value, checking_row+1, size);
									if(equal_flag &&(value==atoi(tempcurr->tok_string))) rowneeded0=true;
									if(less_flag &&(value < atoi(tempcurr->tok_string))) rowneeded0=true;
									if(great_flag &&(value > atoi(tempcurr->tok_string))) rowneeded0=true;
								}
								else
								{
									rc = INCORRECT_DATA_TYPE;
									tempcurr->tok_value = INVALID;
									return rc;
								}
							}
							else if(column->col_type==T_CHAR)
							{													
								if(tempcurr->tok_value==STRING_LITERAL)
								{
									memset(strValue, '\0', MAX_TOK_LEN);
									memcpy(strValue, checking_row+1, size);
									if(equal_flag &&!(stricmp(strValue,tempcurr->tok_string))) rowneeded0=true;
									if(less_flag)
									{
										if((stricmp(strValue,tempcurr->tok_string)<0)) 
										{rowneeded0=true;}
									}
									if(great_flag)
									{
										if((stricmp(strValue,tempcurr->tok_string)>0))
										{rowneeded0=true;}
									}
								}
								else
								{
									rc = INCORRECT_DATA_TYPE;
									tempcurr->tok_value = INVALID;
									return rc;
								}
							}
						}
						//q++;  
					}


					checking_row = (char*)(checking_row+1+column->col_len);
					column++;			
					j++;
				}

				if(rowneeded0==true) requiredrow=true;
				column = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);							

				checking_row = backupRow;//i*table_file->record_size;
				j=0;

				if(!requiredrow)
				{
					temp_table_file->num_records=temp_table_file->num_records+1;
					temp_table_file->file_size=temp_table_file->file_size+table_file->record_size;

					memcpy((void*) ((char*) temp_table_file+ offset),(void *)checking_row,table_file->record_size);
					offset=offset+table_file->record_size;
					//memcpy(filerow + offset, &nullSize, 1);


				}	

				checking_row=(char*)(checking_row+table_file->record_size);
				i++;

			}

			FILE *fhandle1=NULL;

			//fclose(fhandle);
			//fhandle1 = fopen(filename, "rbc");

			//if(fhandle1==NULL)
			//{
			fhandle1 = fopen(filename, "wbc");
			if(fhandle1!=NULL){
				fwrite(temp_table_file, temp_table_file -> file_size, 1, fhandle1);
				new_filesize=temp_table_file->file_size;
				fflush(fhandle1);
				fclose(fhandle1);}
			/*else
			{
			rc=FILE_OPEN_ERROR;
			return rc;
			}
			}*/
		}
	
		if(old_filesize!=new_filesize)
		{
			int a=0;
			a=logging();

			if(a==0)
			return rc;
		}
	}
	
	return rc;
}

int sem_backup(token_list *t_list)
{
	int rc = 0;
	char *backup_file;
	int table_length = sizeof(int);
	char file[MAX_IDENT_LEN + 4];
	FILE *backup = NULL;
	FILE *opentab = NULL;
	int tables_num=0;
	int tables_counter = g_tpd_list->num_tables;
	token_list *cur;
	token_list *log;
	tpd_entry temp_table;
	cur=t_list;
	log=t_list;
	struct _stat file_stat;
	
		if ((cur->tok_value != IDENT))	
		{	
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
		}

		else
		{
			
			if (cur->next != NULL && cur->next->tok_value != EOC )
			{
				rc = INVALID_STATEMENT;
				cur->next->tok_value = INVALID;
			}
			else
			{
				backup_file = cur->tok_string;
				tables_num = g_tpd_list->num_tables;
				if((backup = fopen(backup_file, "rbc")) != NULL)
				{
					rc = BACKUPFILE_ALREADY_EXISTS;
					cur->tok_value=INVALID;
					return rc;
				}
				else if((backup = fopen(cur->tok_string, "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
					return rc;
				}
				else
				{
					// writing db.bin contents into backup file
					fwrite(g_tpd_list, g_tpd_list->list_size, 1, backup);
					// pointing to the first table in the list
					tpd_entry *temp_table = &(g_tpd_list->tpd_start);
					if(tables_num==0)
					{
						printf("Tables doesn't exist for backing up");
					}
					else
					{
						while(tables_num>0)
						{
							memset(file, '\0', MAX_IDENT_LEN+4);
							strcpy(file, temp_table->table_name);
							strcat(file, ".tab");
							opentab = fopen(file,"rbc");
							_fstat(_fileno(opentab), &file_stat);  
							table_file = (table_file_header*)calloc(1, file_stat.st_size); // allocating the size

							fread(table_file, file_stat.st_size, 1, opentab);
							fclose(opentab);

							if (table_file->file_size != file_stat.st_size)
							{
								rc = DBFILE_CORRUPTION;
							}
							else
							{
								// table_length is for storing the 4 bytes length of file
								fwrite(&(table_file->file_size),table_length, 1, backup); 	
								// writing the table file content into the backup file
								fwrite(table_file, table_file->file_size, 1, backup);								
							}

							temp_table = (tpd_entry*)((char*)temp_table + temp_table->tpd_size);
							tables_num--;
						}
					}
					printf("Back up file %s is created with %d tables",backup_file,tables_counter);	
					printf("\n");
				}
				//printf("%s",backup_file);
			}
		}
		if(rc==0)
		{
			int a=0;
			a=log_backup_restore(log);

			if(a==0)
			return rc;
		}
		return rc;
}

int sem_restore(token_list *t_list)
{
		int rc = 0;
		char *backup_file;
		int table_length = sizeof(int);
		char file[MAX_IDENT_LEN + 4];
		FILE *backup = NULL;
		FILE *opentab = NULL;
		int tables_num=0;
		int tables_counter = g_tpd_list->num_tables;
		token_list *cur;
		tpd_entry temp_table;
		cur=t_list;
		struct _stat file_stat;
	
		if ((cur->tok_value != IDENT))	
		{	
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
		}

		else
		{
			
			if (cur->next != NULL && cur->next->tok_value != EOC )
			{
				rc = INVALID_STATEMENT;
				cur->next->tok_value = INVALID;
			}
			else
			{
				backup_file=cur->tok_string;
				printf("The requested image for restoration is: %s",backup_file);
			}
		}
	return rc;
}

int sem_drop_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	char filename[MAX_IDENT_LEN+4];
	cur = t_list;
	strcpy(filename,cur->tok_string);
	if ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->next->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}

			else
			{
				/* Found a valid tpd, drop it from tpd list */
				rc = drop_tpd_from_list(cur->tok_string);

				strcat(filename,".tab");
				remove(filename);
			}
		}
	}
	if(rc==0)
	{
		int a=0;
		a=logging();

		if(a==0)
		return rc;
	}
	return rc;
}

int sem_list_tables()
{
	int rc = 0;
	int num_tables = g_tpd_list->num_tables;
	tpd_entry *cur = &(g_tpd_list->tpd_start);

	if (num_tables == 0)
	{
		printf("\nThere are currently no tables defined\n");
	}
	else
	{
		printf("\nTable List\n");
		printf("*****************\n");
		while (num_tables-- > 0)
		{
			printf("%s\n", cur->table_name);
			if (num_tables > 0)
			{
				cur = (tpd_entry*)((char*)cur + cur->tpd_size);
			}
		}
		printf("****** End ******\n");
	}

	return rc;
}

int sem_list_schema(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	char tab_name[MAX_IDENT_LEN+1];
	char filename[MAX_IDENT_LEN+1];
	bool report = false;
	FILE *fhandle = NULL;
	int i = 0;

	cur = t_list;

	if (cur->tok_value != K_FOR)
	{
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		if ((cur->tok_class != keyword) &&
			(cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
		{
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			memset(filename, '\0', MAX_IDENT_LEN+1);
			strcpy(tab_name, cur->tok_string);
			cur = cur->next;

			if (cur->tok_value != EOC)
			{
				if (cur->tok_value == K_TO)
				{
					cur = cur->next;

					if ((cur->tok_class != keyword) &&
						(cur->tok_class != identifier) &&
						(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_REPORT_FILE_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						if (cur->next->tok_value != EOC)
						{
							rc = INVALID_STATEMENT;
							cur->next->tok_value = INVALID;
						}
						else
						{
							/* We have a valid file name */
							strcpy(filename, cur->tok_string);
							report = true;
						}
					}
				}
				else
				{ 
					/* Missing the TO keyword */
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
			}

			if (!rc)
			{
				if ((tab_entry = get_tpd_from_list(tab_name)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					if (report)
					{
						if((fhandle = fopen(filename, "a+tc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
					}

					if (!rc)
					{
						/* Find correct tpd, need to parse column and index information */

						/* First, write the tpd_entry information */
						printf("Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
						printf("Table Name               (table_name)  = %s\n", tab_entry->table_name);
						printf("Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
						printf("Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
						printf("Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 

						if (report)
						{
							fprintf(fhandle, "Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
							fprintf(fhandle, "Table Name               (table_name)  = %s\n", tab_entry->table_name);
							fprintf(fhandle, "Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
							fprintf(fhandle, "Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
							fprintf(fhandle, "Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 
						}

						/* Next, write the cd_entry information */
						for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
							i < tab_entry->num_columns; i++, col_entry++)
						{
							printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
							printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							printf("Column Length (col_len)  = %d\n", col_entry->col_len);
							printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);

							if (report)
							{
								fprintf(fhandle, "Column Name   (col_name) = %s\n", col_entry->col_name);
								fprintf(fhandle, "Column Id     (col_id)   = %d\n", col_entry->col_id);
								fprintf(fhandle, "Column Type   (col_type) = %d\n", col_entry->col_type);
								fprintf(fhandle, "Column Length (col_len)  = %d\n", col_entry->col_len);
								fprintf(fhandle, "Not Null Flag (not_null) = %d\n\n", col_entry->not_null);
							}
						}

						if (report)
						{
							fflush(fhandle);
							fclose(fhandle);
						}
					} // File open error							
				} // Table not exist
			} // no semantic errors
		} // Invalid table name
	} // Invalid statement

	return rc;
}

int initialize_tpd_list()
{
	int rc = 0;
	FILE *fhandle = NULL;
	struct _stat file_stat;

	/* Open for read */
	if((fhandle = fopen("dbfile.bin", "rbc")) == NULL)
	{
		if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
		else
		{
			g_tpd_list = NULL;
			g_tpd_list = (tpd_list*)calloc(1, sizeof(tpd_list));

			if (!g_tpd_list)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				g_tpd_list->list_size = sizeof(tpd_list);
				fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}
	else
	{
		/* There is a valid dbfile.bin file - get file size */
		_fstat(_fileno(fhandle), &file_stat);
		printf("dbfile.bin size = %d\n", file_stat.st_size);

		g_tpd_list = (tpd_list*)calloc(1, file_stat.st_size);

		if (!g_tpd_list)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(g_tpd_list, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);

			if (g_tpd_list->list_size != file_stat.st_size)
			{
				rc = DBFILE_CORRUPTION;
			}

		}
	}

	return rc;
}

int add_tpd_to_list(tpd_entry *tpd)
{
	int rc = 0;
	int old_size = 0;
	FILE *fhandle = NULL;

	if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
	else
	{
		old_size = g_tpd_list->list_size;

		if (g_tpd_list->num_tables == 0)
		{
			/* If this is an empty list, overlap the dummy header */
			g_tpd_list->num_tables++;
			g_tpd_list->list_size += (tpd->tpd_size - sizeof(tpd_entry));
			fwrite(g_tpd_list, old_size - sizeof(tpd_entry), 1, fhandle);
		}
		else
		{
			/* There is at least 1, just append at the end */
			g_tpd_list->num_tables++;
			g_tpd_list->list_size += tpd->tpd_size;
			fwrite(g_tpd_list, old_size, 1, fhandle);
		}

		fwrite(tpd, tpd->tpd_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int drop_tpd_from_list(char *tabname)
{
	int rc = 0;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;
	int count = 0;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (stricmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				int old_size = 0;
				FILE *fhandle = NULL;

				if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
				else
				{
					old_size = g_tpd_list->list_size;

					if (count == 0)
					{
						/* If this is the first entry */
						g_tpd_list->num_tables--;

						if (g_tpd_list->num_tables == 0)
						{
							/* This is the last table, null out dummy header */
							memset((void*)g_tpd_list, '\0', sizeof(tpd_list));
							g_tpd_list->list_size = sizeof(tpd_list);
							fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
						}
						else
						{
							/* First in list, but not the last one */
							g_tpd_list->list_size -= cur->tpd_size;

							/* First, write the 8 byte header */
							fwrite(g_tpd_list, sizeof(tpd_list) - sizeof(tpd_entry),
								1, fhandle);

							/* Now write everything starting after the cur entry */
							fwrite((char*)cur + cur->tpd_size,
								old_size - cur->tpd_size -
								(sizeof(tpd_list) - sizeof(tpd_entry)),
								1, fhandle);
						}
					}
					else
					{
						/* This is NOT the first entry - count > 0 */
						g_tpd_list->num_tables--;
						g_tpd_list->list_size -= cur->tpd_size;

						/* First, write everything from beginning to cur */
						fwrite(g_tpd_list, ((char*)cur - (char*)g_tpd_list),
							1, fhandle);

						/* Check if cur is the last entry. Note that g_tdp_list->list_size
						has already subtracted the cur->tpd_size, therefore it will
						point to the start of cur if cur was the last entry */
						if ((char*)g_tpd_list + g_tpd_list->list_size == (char*)cur)
						{
							/* If true, nothing else to write */
						}
						else
						{
							/* NOT the last entry, copy everything from the beginning of the
							next entry which is (cur + cur->tpd_size) and the remaining size */
							fwrite((char*)cur + cur->tpd_size,
								old_size - cur->tpd_size -
								((char*)cur - (char*)g_tpd_list),							     
								1, fhandle);
						}
					}

					fflush(fhandle);
					fclose(fhandle);
				}


			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
					count++;
				}
			}
		}
	}

	if (!found)
	{
		rc = INVALID_TABLE_NAME;
	}

	return rc;
}

tpd_entry* get_tpd_from_list(char *tabname)
{
	tpd_entry *tpd = NULL;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (stricmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				tpd = cur;
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
				}
			}
		}
	}

	return tpd;
}


//	creation of table file

int init_table_file(char *table_name)
{
	int rc = 0;
	FILE *fhandle = NULL;
	struct _stat file_stat;
	char filename[MAX_IDENT_LEN+4];

	strcpy(filename,table_name);
	strcat(filename,".tab");

	//if ((fhandle = fopen(filename, "rbc")) == NULL)
	//{
	if((fhandle = fopen(filename, "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
	else
	{
		if (!table_file)
		{
			rc = MEMORY_ERROR;
			fclose(fhandle);
		}
		else
		{

			fwrite(table_file, sizeof(table_file_header), 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);

		}		
	}

	fhandle = fopen(filename, "rbc");
	_fstat(_fileno(fhandle), &file_stat);
	printf("file size of %s is %d\n", filename,file_stat.st_size);//sizeof(table_file_header) );
	fflush(fhandle);
	fclose(fhandle);
	return rc;
	//}
}

int logging()
{
	int rc=0;
	char *log_record=NULL;
	// declarations and operations for time stamp
	
	time_t null_time;
	struct tm *lcl_time=NULL;
	char tymstamp[30];
	null_time = time(NULL);
	lcl_time = localtime(&null_time);
	strftime(tymstamp, 15, "%Y%m%d%H%M%S", lcl_time);
	int size=strlen(query)+20;

	// declarations for writing to log
	
	token_list *temp;
	FILE *logfile=NULL;
	struct _stat file_stat;

	// writing to log

	//if ((logfile = fopen("db.log", "r")) == NULL)
	//{

		if ((logfile = fopen("db.log", "a")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}

		else
		{
			log_record = (char *)calloc(size, sizeof(char));
			// for copying the timestamp
			strcpy(log_record, tymstamp);
			// for copying the 'space' and 'start double quotes'
			strcat(log_record, " \"");		
			// for copying the query
			strcat(log_record, query);
			// for copying the 'end double quotes'
			strcat(log_record, "\"\n");
			// for copying the log entry into db.log file
			fputs(log_record, logfile);

			fflush(logfile);
			fclose(logfile);
		}
	
	return rc;
}

int log_backup_restore(token_list *t_list)
{
	int rc=0;
	char *log_record=NULL;
	char *image_name;
	// declarations and operations for time stamp
	
	time_t null_time;
	struct tm *lcl_time=NULL;
	char tymstamp[30];
	null_time = time(NULL);
	lcl_time = localtime(&null_time);
	strftime(tymstamp, 15, "%Y%m%d%H%M%S", lcl_time);

	// declarations for writing to log
	
	token_list *temp;
	temp = t_list;
	image_name=temp->tok_string;

	FILE *logfile=NULL;
	
	int size = strlen("BACKUP ")+strlen(image_name)+2;
	// writing to log

	
		if ((logfile = fopen("db.log", "a")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}

		else
		{
			log_record = (char *)calloc(size, sizeof(char));
			// for copying the 'BACKUP'
			strcpy(log_record, "BACKUP ");
			// for concatenating the 'image name'
			strcat(log_record, image_name);
			// for adding the new line
			strcat(log_record, "\n");

			fputs(log_record, logfile);

			fflush(logfile);
			fclose(logfile);
		}
	
	return rc;
}