////////////////////////////////////////////////////////////////////////////////
//
//  File          : crud_client.c
//  Description   : This is the client side of the CRUD communication protocol.
//
//  Author       : Xuejian Zhou
//  Last Modified : Fri April 28 2017
//

// Include Files

// Project Include Files
#include <crud_network.h>
#include <cmpsc311_log.h>
#include <cmpsc311_util.h>
#include <signal.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>

// Global variables
int            crud_network_shutdown = 0; // Flag indicating shutdown
unsigned char *crud_network_address = NULL; // Address of CRUD server 
unsigned short crud_network_port = 0; // Port of CRUD server

int sockfd;
struct sockaddr_in caddr;


////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_client_operation
// Description  : This the client operation that sends a request to the CRUD
//                server.   It will:
//
//                1) if INIT make a connection to the server
//                2) send any request to the server, returning results
//                3) if CLOSE, will close the connection
//
// Inputs       : op - the request opcode for the command
//                buf - the block to be read/written from (READ/WRITE)
// Outputs      : the response structure encoded as needed

CrudResponse crud_client_operation(CrudRequest op, void *buf) {
	

	uint64_t response;
	int type;
    int type2;
	int length;
	int counter = 0;
	int type3;
	int length2;
	int total;
	uint64_t response2;
    
    //extract op to get the type 
	type = ((op)<<32)>>60 ;

	//when type is init
	if(type == CRUD_INIT){

		//creating a socket
		sockfd = socket(PF_INET, SOCK_STREAM, 0);
		if(sockfd == -1){
			printf("Error on socket creation [%s]\n", strerror(errno));
			return (-1);
		}
		
		struct sockaddr_in caddr;
		caddr.sin_family = AF_INET;
		caddr.sin_port = htons(CRUD_DEFAULT_PORT);
		if(inet_aton(CRUD_DEFAULT_IP, &caddr.sin_addr) == 0){
				return(-1);
		}

		//connect
		if( connect(sockfd, (const struct sockaddr*)&caddr, sizeof(struct sockaddr)) == -1){
			return (-1);
		}

	}

	// start to send the request to server
	

	// extract the req to get the tyep and length
	type2 =  ((op)<<32)>>60 ;
 	length =  ((op)<<36)>>40;

	// conver the type to the network byte order
	uint64_t network = htonll64(op);

	// start to write 
	write(sockfd, &network, sizeof(network));


	// when the type is create
	if (type2 == CRUD_CREATE ){
		
		write(sockfd, &buf[counter], length - counter);
			
	}

	// when the type is update
	if(type2 == CRUD_UPDATE){
		
		write(sockfd, &buf[counter], length - counter);
			
	}

	///////send is over in here
	///////// start to receive



	// extract the req to get the tyep and length
	type3 =  ((op)<<32)>>60 ;
 	length2 =  ((op)<<36)>>40;

	// start to read
	read(sockfd, &response2, sizeof(response2));

	// Convert NBO to HBO
	ntohll64(response2);

	// when type is READ
	if (type3 == CRUD_READ){

		total = 0;

		while(total != length2){

			int counter2;

			counter2 =read(sockfd, &buf[total], length2-total);

			total = total + counter2;
		} 

	} 

	uint64_t reader = ntohll64(response2);
	


	//when type is close
	if(type == CRUD_CLOSE){
		close(sockfd);
		sockfd = -1;
		

	}

	return reader;
}








 	


















