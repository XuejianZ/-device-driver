////////////////////////////////////////////////////////////////////////////////
//
//  File           : crud_file_io.c
//  Description    : This is the implementation of the standardized IO functions
//                   for used to access the CRUD storage system.
//
//  Author         : Xuejian Zhou
//  Last Modified  : Friday April 28 2017
//

// Includes
#include <malloc.h>
#include <string.h>
#include <stdlib.h>
#include <crud_file_io.h>
#include <crud_driver.h>
#include <cmpsc311_log.h>
#include <cmpsc311_util.h>
#include <crud_network.h>

#define CIO_UNIT_TEST_MAX_WRITE_SIZE 1024
#define CRUD_IO_UNIT_TEST_ITERATIONS 10240

// when the flag is 0 , it means the curd is not initialzed yet 
int flag = 0 ;

// Type for UNIT test interface
typedef enum {
    CIO_UNIT_TEST_READ   = 0,
    CIO_UNIT_TEST_WRITE  = 1,
    CIO_UNIT_TEST_APPEND = 2,
    CIO_UNIT_TEST_SEEK   = 3,
} CRUD_UNIT_TEST_TYPE;


typedef struct{

	uint64_t OID;
	uint64_t Req;
	uint64_t Length;
	uint64_t Flags;
	uint64_t R;

}Cruid;


typedef struct{
	
	char filename[128];
	uint32_t oid;
	uint32_t length;
	uint8_t open;
	uint32_t current_position;

}file;

file File[1024];


////////////////////////////////////////////////////////////////////////////////
//
// Function     : create_crude_opcode
// Description  : This function can create a 64 bits opcode by shifting the bits and binding them.
//
// Inputs       : OID    64bits object identifer, 
//				  Req    64bits Request type,
//				  Length 64bits the length of the file
//				  Flags  64bits unused in this assignment
//				  R      64bits return value, o is success , 1 is failer
//
// Outputs      : one 64bits opcode


uint64_t create_crude_opcode(uint64_t OID,uint64_t Req,uint64_t Length,uint64_t Flags,uint64_t R){

	
	uint64_t sum;

	//Now start to shift the bits 

	OID = (OID)<<32;

	Req = (Req)<<28;

	Length = (Length)<<4;

	Flags = (Flags)<<1;

	R= (R);

	sum = OID | Req | Length| Flags| R ;

	return sum;

}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : extract_crude_opcode
// Description  : This function can extract the 64bits response into the five 64bits opcode which are OID, Req,
//                Length, Flags, and R by shifting the bits 
//
// Inputs       : 64bits response opcode

//
// Outputs      : Cruid structre which has OID , Req, Length, Flags and R.

Cruid extract_crude_opcode(uint64_t response){

	//create  a object 
	Cruid extractValue;

	//extract the 64bis response into five values.
	extractValue.OID = response;
	extractValue.OID = (extractValue.OID )>>32;

	extractValue.Req = response;
	extractValue.Req = (extractValue.Req)<<32;
	extractValue.Req = (extractValue.Req)>>60;

	extractValue.Length = response;
	extractValue.Length = (extractValue.Length)<<36;
	extractValue.Length = (extractValue.Length)>>40;

	extractValue.Flags = response;
	extractValue.Flags = (extractValue.Flags)<<60;
	extractValue.Flags = (extractValue.Flags)>>61;

	extractValue.R = response;
	extractValue.R = (extractValue.R)<<63;
	extractValue.R = (extractValue.R)>>63;

	return  extractValue;

}


////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_open
// Description  : This function opens the file
//
// Inputs       : path 
// Outputs      : file 

int16_t crud_open(char *path) {


    uint64_t send;

  
    int fd = 0;

    //when the flag is 0 , it means the curd is not initialzed yet 
    if (flag == 0)
    {
       
        send= create_crude_opcode(0, CRUD_INIT, 0, 0, 0); 
        crud_client_operation(send, NULL);
        flag = 1;
    }

    if(fd < CRUD_MAX_TOTAL_FILES){
        fd ++ ;
    }


    if(fd == CRUD_MAX_TOTAL_FILES){

         while (strcmp(File[fd].filename, "") > 0)
            fd++;

        strcpy(File[fd].filename, path);
        File[fd].oid = 0;
        File[fd].current_position = 0;
        File[fd].length = 0;
        File[fd].open = 1;


  }

        while (strcmp(File[fd].filename, "") > 0)
            fd++;
        
            strcpy(File[fd].filename, path);
            File[fd].oid = 0;
            File[fd].current_position = 0;
            File[fd].length = 0;
            File[fd].open = 1;
    
    return fd; 
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_close
// Description  : This function closes the file
//
// Inputs       : fd - the file handle of the object to close
// Outputs      : 0 if successful, -1 if failure

int16_t crud_close(int16_t fd) {

	uint64_t send; 

    //when the flag is 0 , it means the curd is not initialzed yet 
	
	if (flag == 0){
       
        //first send the intial Request type to the crude by calling the bus 
		send = create_crude_opcode(0,CRUD_INIT, 0,0,0);

		crud_client_operation(send, NULL);

		//now the flag is 1 , it means the crud is initialized
		flag =1 ;

		}

		//set open to 0 to close the file
		File[fd].open = 0 ;


		return (0);

}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_read
// Description  : Reads up to "count" bytes from the file handle "fh" into the
//                buffer  "buf".
//
// Inputs       : fd - the file descriptor for the read
//                buf - the buffer to place the bytes into
//                count - the number of bytes to read
// Outputs      : the number of bytes read or -1 if failures

int32_t crud_read(int16_t fd, void *buf, int32_t count) {
 
	

 	uint64_t send ;

 	//when the flag is 0 , it means the curd is not initialized yet 
	if(flag == 0){

		send = create_crude_opcode(0,CRUD_INIT,0,0,0);

		crud_client_operation(send, NULL);


		// it is initialized now
		flag = 1;

	}	


	if(File[fd].oid == 0 )
		return 0;


	//aftering reading this amount of data, the current position is still less then total length
	if(File[fd].current_position + count <= File[fd].length){

		//create a temporary buf
		char *temp_buff = malloc(File[fd].length);
		// send the READ request type and length
		send = create_crude_opcode(File[fd].oid,CRUD_READ,File[fd].length,0,0);
        //  read the previous data first 
		crud_client_operation(send, temp_buff);
		// keeping reading the counts bytes from the previous position by memory copying 
		memcpy(buf, &temp_buff[File[fd].current_position], count);
		//reset the current position 
		File[fd].current_position = File[fd].current_position + count ;

		//printf("read successful!!!!!!!!!!!!!!!!!!!!!");
		//free the memory 
		free(temp_buff);
		//return the count read
		return (count) ; 
		
	}

	//aftering reading this amount of data, the current position become final position
	else{

		int actual_count;
		//create a temporary buf
		char *temp_buff = malloc(File[fd].length);
		// send the READ request type and length
		send = create_crude_opcode(File[fd].oid, CRUD_READ,File[fd].length,0,0);
		//  read the previous data first 
		crud_client_operation(send, temp_buff);
		//This is the actual bytes read 
		actual_count = File[fd].length - File[fd].current_position;
		// keeping reading the counts bytes from the previous position by memory copying 
		memcpy(buf, &temp_buff[File[fd].current_position], actual_count );
        //reset the current position
		File[fd].current_position = File[fd].length;

		//printf("read successful!!!!!!!!!!!!!!!!!!!!!");
        //free the memory
		free(temp_buff);
		//return the actual count read
		return (actual_count);
		
	}

}


//////////////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_write
// Description  : Writes "count" bytes to the file handle "fh" from the
//                buffer  "buf"
//
// Inputs       : fd - the file descriptor for the file to write to
//                buf - the buffer to write
//                count - the number of bytes to write
// Outputs      : the number of bytes written or -1 if failure

int32_t crud_write(int16_t fd, void *buf, int32_t count) {

	uint64_t send, accept;

 
 	//when the flag is 0 , it means the curd is not initialized yet 
    if (flag == 0){
        
        uint64_t send = create_crude_opcode(0, CRUD_INIT, 0, 0, 0); 

        crud_client_operation(send, NULL);

        // it is initialized now
      	flag = 1; 


    }

     
    // There are two cases , one is when object does not exist , and another one the object exist.

    // When the object does not exist

    if (File[fd].oid == 0){
        //  create a new object by using sending CRUD_CREATE type request
        uint64_t send = create_crude_opcode(0, CRUD_CREATE, count, 0, 0);
        //  calling the bus 
        uint64_t accept = crud_client_operation(send, buf);

        Cruid new = extract_crude_opcode(accept);
        // reset the oid , length and current length 
      	File[fd].oid = new.OID;

        File[fd].length = count;

        File[fd].current_position = count; 

        return count; 
	}

    // When the obejct exist already
    else {
        


     
        // when writing beyond the total length of the file
        if (File[fd].current_position + count > File[fd].length)  {

        	// read the current data in the file frist 
            send = create_crude_opcode(File[fd].oid, CRUD_READ, File[fd].length, 0, 0);

       		char *temp_read_buff = malloc(File[fd].length); 

       	    crud_client_operation(send, temp_read_buff);


			// create a temp buff
            char *temp_buff = malloc(File[fd].current_position + count);

            // First copy old data into new  buff first 
            memcpy(temp_buff, temp_read_buff, File[fd].length);

            // Then copy new data into buw buff  
            memcpy(&temp_buff[File[fd].current_position], buf, count);

            // Now Create new object
            send = create_crude_opcode(0, CRUD_CREATE, File[fd].current_position + count, 0, 0);

            //calling the bus 
            accept = crud_client_operation(send, temp_buff);

            // extract the accept 
            Cruid new = extract_crude_opcode(accept);
           
            // Free memory
			free(temp_read_buff);

            free(temp_buff);
          

            // Delete old object
            send = create_crude_opcode(File[fd].oid, CRUD_DELETE, 0, 0, 0);
            //calling the bus 
            crud_client_operation(send, NULL);
           
            
            // reset the oid , length , and current position
            File[fd].oid =  new.OID;

            File[fd].length = File[fd].current_position + count;

            File[fd].current_position += count;

            
            return count;

        }
        
        // aftering  writing counts  it still less than the length of the file
        else {
        	// read the current data in the file frist 
        	send = create_crude_opcode(File[fd].oid, CRUD_READ, File[fd].length, 0, 0);

       		char *temp_read_buff = malloc(File[fd].length); 
       		//calling the bus 
       	    accept = crud_client_operation(send, temp_read_buff);

            // Copy new data  into the buff
            memcpy(&temp_read_buff[File[fd].current_position], buf, count);

            // update the object 
            send = create_crude_opcode(File[fd].oid, CRUD_UPDATE, File[fd].length, 0, 0);

            accept = crud_client_operation(send, temp_read_buff);

            extract_crude_opcode(accept);

            // Free memory
            free(temp_read_buff);
            
         
         	File[fd].current_position += count;

            
            return (count);
        }
    }


  }  

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_seek
// Description  : Seek to specific point in the file
//
// Inputs       : fd - the file descriptor for the file to seek
//                loc - offset from beginning of file to seek to
// Outputs      : 0 if successful or -1 if failure

int32_t crud_seek(int16_t fd, uint32_t loc) {

	
	//when the flag is 0 , it means the curd is not initialized yet 

	if(flag == 0){

        uint64_t send;  

        send = create_crude_opcode(0,CRUD_INIT, 0,0,0);

        crud_client_operation(send, NULL);
	
		// it is initialized now
		flag = 1;
    }

    	//after seek , the current position is local position
    	File[fd].current_position = loc;


    	return (0);

}


////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_format
// Description  : This function formats the crud drive, and adds the file
//                allocation table.
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

uint16_t crud_format(void) {
    
  
    uint64_t send;

    //when the flag is 0 , it means the curd is not initialized yet 
    if (flag == 0){
        
        send = create_crude_opcode(0, CRUD_INIT, 0, 0, 0); 
        crud_client_operation(send, NULL);
        flag = 1;
    }

    // Format
    send = create_crude_opcode(0, CRUD_FORMAT, 0, CRUD_NULL_FLAG, 0);
    crud_client_operation(send, NULL);
    // Create priority object 
    send = create_crude_opcode(0, CRUD_CREATE, CRUD_MAX_TOTAL_FILES*sizeof(file), CRUD_PRIORITY_OBJECT, 0);
    crud_client_operation(send, File);
    return(0);
    
}


////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_mount
// Description  : This function mount the current crud file system and loads
//                the file allocation table.
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure


uint16_t crud_mount(void) {

    uint64_t send;

   //when the flag is 0 , it means the curd is not initialized yet 
    if (flag == 0){
       
        send = create_crude_opcode(0, CRUD_INIT, 0, 0, 0); 
        crud_client_operation(send, NULL);
        flag = 1;
    }

    // Now, read priority object, then load file table
    send = create_crude_opcode(0, CRUD_READ, CRUD_MAX_TOTAL_FILES*sizeof(file), CRUD_PRIORITY_OBJECT, 0);
    crud_client_operation(send, File);
  

    return(0);
}




////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_unmount
// Description  : This function unmounts the current crud file system and
//                saves the file allocation table.
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

uint16_t crud_unmount(void) {

    uint64_t send;

    // Update priority object 
    send = create_crude_opcode(0, CRUD_UPDATE, CRUD_MAX_TOTAL_FILES*sizeof(file), CRUD_PRIORITY_OBJECT, 0);
    crud_client_operation(send, File);
    
    //  call the close comand
    send= create_crude_opcode(0, CRUD_CLOSE, 0, CRUD_NULL_FLAG, 0);
    crud_client_operation(send, NULL);
   
    return (0);
}



// Module local methods

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crudIOUnitTest
// Description  : Perform a test of the CRUD IO implementation
//
// Inputs       : None
// Outputs      : 0 if successful or -1 if failure

int crudIOUnitTest(void) {

	// Local variables
	uint8_t ch;
	int16_t fh, i;
	int32_t cio_utest_length, cio_utest_position, count, bytes, expected;
	char *cio_utest_buffer, *tbuf;
	CRUD_UNIT_TEST_TYPE cmd;
	char lstr[1024];

	// Setup some operating buffers, zero out the mirrored file contents
	cio_utest_buffer = malloc(CRUD_MAX_OBJECT_SIZE);
	tbuf = malloc(CRUD_MAX_OBJECT_SIZE);
	memset(cio_utest_buffer, 0x0, CRUD_MAX_OBJECT_SIZE);
	cio_utest_length = 0;
	cio_utest_position = 0;

	// Format and mount the file system
	if (crud_format() || crud_mount()) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure on format or mount operation.");
		return(-1);
	}

	// Start by opening a file
	fh = crud_open("temp_file.txt");
	if (fh == -1) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure open operation.");
		return(-1);
	}

	// Now do a bunch of operations
	for (i=0; i<CRUD_IO_UNIT_TEST_ITERATIONS; i++) {

		// Pick a random command
		if (cio_utest_length == 0) {
			cmd = CIO_UNIT_TEST_WRITE;
		} else {
			cmd = getRandomValue(CIO_UNIT_TEST_READ, CIO_UNIT_TEST_SEEK);
		}

		// Execute the command
		switch (cmd) {

		case CIO_UNIT_TEST_READ: // read a random set of data
			count = getRandomValue(0, cio_utest_length);
			logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : read %d at position %d", bytes, cio_utest_position);
			bytes = crud_read(fh, tbuf, count);
			if (bytes == -1) {
				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Read failure.");
				return(-1);
			}

			// Compare to what we expected
			if (cio_utest_position+count > cio_utest_length) {
				expected = cio_utest_length-cio_utest_position;
			} else {
				expected = count;
			}
			if (bytes != expected) {
				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : short/long read of [%d!=%d]", bytes, expected);
				return(-1);
			}
			if ( (bytes > 0) && (memcmp(&cio_utest_buffer[cio_utest_position], tbuf, bytes)) ) {

				bufToString((unsigned char *)tbuf, bytes, (unsigned char *)lstr, 1024 );
				logMessage(LOG_INFO_LEVEL, "CIO_UTEST R: %s", lstr);
				bufToString((unsigned char *)&cio_utest_buffer[cio_utest_position], bytes, (unsigned char *)lstr, 1024 );
				logMessage(LOG_INFO_LEVEL, "CIO_UTEST U: %s", lstr);

				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : read data mismatch (%d)", bytes);
				return(-1);
			}
			logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : read %d match", bytes);


			// update the position pointer
			cio_utest_position += bytes;
			break;

		case CIO_UNIT_TEST_APPEND: // Append data onto the end of the file
			// Create random block, check to make sure that the write is not too large
			ch = getRandomValue(0, 0xff);
			count =  getRandomValue(1, CIO_UNIT_TEST_MAX_WRITE_SIZE);
			if (cio_utest_length+count >= CRUD_MAX_OBJECT_SIZE) {

				// Log, seek to end of file, create random value
				logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : append of %d bytes [%x]", count, ch);
				logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : seek to position %d", cio_utest_length);
				if (crud_seek(fh, cio_utest_length)) {
					logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : seek failed [%d].", cio_utest_length);
					return(-1);
				}
				cio_utest_position = cio_utest_length;
				memset(&cio_utest_buffer[cio_utest_position], ch, count);

				// Now write
				bytes = crud_write(fh, &cio_utest_buffer[cio_utest_position], count);
				if (bytes != count) {
					logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : append failed [%d].", count);
					return(-1);
				}
				cio_utest_length = cio_utest_position += bytes;
			}
			break;

		case CIO_UNIT_TEST_WRITE: // Write random block to the file
			ch = getRandomValue(0, 0xff);
			count =  getRandomValue(1, CIO_UNIT_TEST_MAX_WRITE_SIZE);
			// Check to make sure that the write is not too large
			if (cio_utest_length+count < CRUD_MAX_OBJECT_SIZE) {
				// Log the write, perform it
				logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : write of %d bytes [%x]", count, ch);
				memset(&cio_utest_buffer[cio_utest_position], ch, count);
				bytes = crud_write(fh, &cio_utest_buffer[cio_utest_position], count);
				if (bytes!=count) {
					logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : write failed [%d].", count);
					return(-1);
				}
				cio_utest_position += bytes;
				if (cio_utest_position > cio_utest_length) {
					cio_utest_length = cio_utest_position;
				}
			}
			break;

		case CIO_UNIT_TEST_SEEK:
			count = getRandomValue(0, cio_utest_length);
			logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : seek to position %d", count);
			if (crud_seek(fh, count)) {
				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : seek failed [%d].", count);
				return(-1);
			}
			cio_utest_position = count;
			break;

		default: // This should never happen
			CMPSC_ASSERT0(0, "CRUD_IO_UNIT_TEST : illegal test command.");
			break;

		}

#if DEEP_DEBUG
		// VALIDATION STEP: ENSURE OUR LOCAL IS LIKE OBJECT STORE
		CrudRequest request;
		CrudResponse response;
		CrudOID oid;
		CRUD_REQUEST_TYPES req;
		uint32_t length;
		uint8_t res, flags;

		// Make a fake request to get file handle, then check it
		request = construct_crud_request(crud_file_table[0].object_id, CRUD_READ, CRUD_MAX_OBJECT_SIZE, CRUD_NULL_FLAG, 0);
		response = crud_bus_request(request, tbuf);
		if ((deconstruct_crud_request(response, &oid, &req, &length, &flags, &res) != 0) || (res != 0))  {
			logMessage(LOG_ERROR_LEVEL, "Read failure, bad CRUD response [%x]", response);
			return(-1);
		}
		if ( (cio_utest_length != length) || (memcmp(cio_utest_buffer, tbuf, length)) ) {
			logMessage(LOG_ERROR_LEVEL, "Buffer/Object cross validation failed [%x]", response);
			bufToString((unsigned char *)tbuf, length, (unsigned char *)lstr, 1024 );
			logMessage(LOG_INFO_LEVEL, "CIO_UTEST VR: %s", lstr);
			bufToString((unsigned char *)cio_utest_buffer, length, (unsigned char *)lstr, 1024 );
			logMessage(LOG_INFO_LEVEL, "CIO_UTEST VU: %s", lstr);
			return(-1);
		}

		// Print out the buffer
		bufToString((unsigned char *)cio_utest_buffer, cio_utest_length, (unsigned char *)lstr, 1024 );
		logMessage(LOG_INFO_LEVEL, "CIO_UTEST: %s", lstr);
#endif

	}

	// Close the files and cleanup buffers, assert on failure
	if (crud_close(fh)) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure read comparison block.", fh);
		return(-1);
	}
	free(cio_utest_buffer);
	free(tbuf);

	// Format and mount the file system
	if (crud_unmount()) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure on unmount operation.");
		return(-1);
	}

	// Return successfully
	return(0);
}

































