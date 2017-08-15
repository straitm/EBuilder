#ifndef __USBStreamUtils__
#define __USBStreamUtils__

#include "USBstream-TypeDef.h"

#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <vector>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <syslog.h>
#include <dirent.h>
#include <errno.h>
#include <time.h>

//#include<GaibuClient.cxx>

#define BUFSIZE   1024
#define MNAME            -1
#define MINSTRUCTION      1
#define MNOTICE           2
#define MWARNING          3
#define MERROR            4
#define MEXCEPTION        5

static int gaibu_sockfd;
static struct sockaddr_in gaibu_serveraddr;
static long int GAIBU_PORTNUMBER;
static char GAIBU_SERVER_IP[BUFSIZE];
static char val[BUFSIZE];
static char gaibu_debug_msg[BUFSIZE];

//get the configuration from the DCSpaceIP.config file --
// read IP configuration from file

inline char* config_string(const char* path, const char* key)
{
  char ch[300];
  char name[200];

  char IP[200];
  char port[200];

  FILE * file;
  file = fopen(path,"r");
  if (file != NULL) {
    while( fgets( ch, 120, file ) != NULL ) {
      if (sscanf(ch, "%s :%s", name, IP) == 2) { // get IP/hostname
        if (  strcmp(name, key) == 0 ) {
          sprintf(val,IP);
          fclose(file);
          return val;
        }
      }
      else if (sscanf(ch, "%s %s", name, port) == 2){ // get port number
        if ( strcmp(name, key) == 0 ) {
          sprintf(val,port);
          fclose(file);
          return val;
        }
      }

    }
    fclose(file);
  }
  else {
    printf("Unable to open file %s\n",path);
    exit(1);
  }
  return NULL;
}

inline int config_int(const char* path, const char* key)
{
  return atoi(config_string(path,key));
}

inline float config_float(const char* path, const char* key)
{
  return atof(config_string(path,key));
}

// Load config data
inline void get_gaibu_config()
{
  char spaceIP_path[BUFSIZE];
  sprintf(spaceIP_path,"%s/config/DCSpaceIP.config",getenv("DCONLINE_PATH"));

  //GAIBU_SERVER_IP[BUFSIZE];
  sprintf(GAIBU_SERVER_IP,"%s",config_string(spaceIP_path,"DCGAIBU_IP"));
  printf("Gaibu ip: %s\n",GAIBU_SERVER_IP);

  char spacePort_path[BUFSIZE];
  sprintf(spacePort_path,"%s/config/DCSpacePort.config",getenv("DCONLINE_PATH"));
  GAIBU_PORTNUMBER = config_int(spacePort_path,"DCGAIBU_PORT");
  printf("Gaibu port: %ld\n",GAIBU_PORTNUMBER);

}

// initialize socket
inline int init_socket(struct sockaddr_in *serveraddr, char *hostname, int portno)
{
  int sockfd;
  struct hostent *server;

  sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0) {
    printf("ERROR opening socket");
    //error("ERROR opening socket");
  }

  /* gethostbyname: get the server's DNS entry */
  server = gethostbyname(hostname);
  if (server == NULL) {
    fprintf(stderr,"ERROR, no such host as %s\n", hostname);
    exit(0);
  }

  /* build the server's Internet address */
  bzero((char *) &(*serveraddr), sizeof((*serveraddr)));
  (*serveraddr).sin_family = AF_INET;
  //memcpy(&(*serveraddr)->sin_addr,server->h_addr,server->h_length);
  bcopy((char *)server->h_addr,
      (char *)&(*serveraddr).sin_addr.s_addr, server->h_length);

  (*serveraddr).sin_port = htons(portno);
  return sockfd;
}

// system call to send mail using mailx and ebuilder_notify.sh sript in DCOV/tools
inline void send_mail(int priority, char *gaibu_buf, int run_num = 9999999)
{
  //Email format:
  // Sub: Process Name - Error level
  // Body:
  // date
  // time
  // RunNumber if present
  // Error Level
  // Message

  char mail_message[BUFSIZE];
  char mail_command[BUFSIZE];

  time_t rawtime;
  struct tm * timeinfo;
  time(&rawtime);
  timeinfo = localtime ( &rawtime );

  //sprintf(mail_message,"%sRun: %d\nError: %d\n%s",ctime(&rawtime),run_num,priority,gaibu_buf);
  sprintf(mail_message,"%.4d%.2d%.2d\n%.2d:%.2d:%.2d\n%.7d\nOV EBuilder\n%s",
      1900+timeinfo->tm_year,
      (1+timeinfo->tm_mon),
      timeinfo->tm_mday,
      timeinfo->tm_hour,
      timeinfo->tm_min,
      timeinfo->tm_sec,
      run_num,
      gaibu_buf);
  //printf("mail_message: %s\n",mail_message);
  sprintf(mail_command,". %s/DCOV/tools/ebuilder_notify.sh %d '%s'",getenv("DCONLINE_PATH"),priority,mail_message);
  //printf("mail_command: %s\n",mail_command);
  system(mail_command);
}

// Send gaibu message and write to the syslog
inline int gaibu_msg(int priority, char *gaibu_buf, std::string myRunNumber="")
{
  int m;
  char gaibu_msg_buf[BUFSIZE];

  if(gaibu_sockfd < 0) {
    gaibu_sockfd = init_socket(&gaibu_serveraddr,GAIBU_SERVER_IP,GAIBU_PORTNUMBER);
    if(gaibu_sockfd>0) {
      if (connect(gaibu_sockfd,(struct sockaddr *) &gaibu_serveraddr, sizeof(gaibu_serveraddr)) < 0) {
        gaibu_sockfd = -1;
        sprintf(gaibu_msg_buf,"Gaibu server connection broken. Unsent message: %s\n",gaibu_buf);
        syslog(LOG_ERR, gaibu_msg_buf);
        printf("Error connecting to gaibu server\n");
      } else {
        syslog(LOG_NOTICE,"OV EBuilder connected to gaibu server\n");
        sprintf(gaibu_msg_buf,"OVEBuilder\n");

        m = send(gaibu_sockfd, gaibu_msg_buf, strlen(gaibu_msg_buf),MSG_NOSIGNAL);
        if (m < 0) {
          syslog(LOG_ERR,"Error writing to Gaibu Socket\n");
          printf("Error writing to Gaibu Socket\n");
          return -1;
        }
      }
    }
    else {
      syslog(LOG_ERR,"Error initializing gaibu socket\n");
      printf("Error initializing gaibu socket\n");
    }
  }
  if(gaibu_sockfd >= 0) {
    sprintf(gaibu_msg_buf,"%d %s\n",priority,gaibu_buf);

    m = send(gaibu_sockfd, gaibu_msg_buf, strlen(gaibu_msg_buf),MSG_NOSIGNAL);

    if (m < 0) {
      syslog(LOG_ERR,"Error writing to gaibu socket\n");
      printf("Error writing to gaibu socket\n");
      gaibu_sockfd = init_socket(&gaibu_serveraddr,GAIBU_SERVER_IP,GAIBU_PORTNUMBER);

      if(gaibu_sockfd >= 0) {
        if (connect(gaibu_sockfd,(struct sockaddr *) &gaibu_serveraddr, sizeof(gaibu_serveraddr)) < 0) {

          gaibu_sockfd = -1;
          sprintf(gaibu_msg_buf,"Gaibu server connection broken. Unsent message: %s\n",gaibu_buf);
          syslog(LOG_ERR, gaibu_msg_buf);
          printf("Error connecting to gaibu server\n");
        } else {
          syslog(LOG_NOTICE,"OV EBuilder connected to gaibu server\n");
          sprintf(gaibu_msg_buf,"OVEBuilder\n");

          m = send(gaibu_sockfd, gaibu_msg_buf, strlen(gaibu_msg_buf),MSG_NOSIGNAL);
          if (m < 0) {
            syslog(LOG_ERR,"Error writing to gaibu socket\n");
            printf("Error writing to gaibu socket\n");
            return -1;
          }
        }
      }
      else {
        syslog(LOG_ERR,"Error initializing gaibu socket\n");
        printf("Error initializing gaibu socket\n");
      }
      //      return -1;
    }

    if(priority > 1) {
      if(priority == 2) { syslog(LOG_NOTICE, gaibu_msg_buf); }
      else if(priority == 3) { syslog(LOG_WARNING, gaibu_msg_buf); }
      else if(priority == 4) { syslog(LOG_ERR, gaibu_msg_buf); }
      else {
        syslog(LOG_CRIT, gaibu_msg_buf);
        if(myRunNumber.empty()) {
          send_mail(priority,gaibu_buf);
        } else {
          send_mail(priority,gaibu_buf,atoi(myRunNumber.c_str()));
        }
      }
    }
  }

  return 0;
}

// Start gaibu
inline void start_gaibu()
{
  openlog("OV EBuilder", LOG_NDELAY, LOG_USER);

  gaibu_sockfd = -1;

  get_gaibu_config();

  sprintf(gaibu_debug_msg,"OV Event Builder Started");

  //gaibu_msg(MNOTICE, gaibu_debug_msg);
}

inline bool LessThan(DataPacket lhs, DataPacket rhs, int ClockSlew=0)
{
  if( lhs.size() < 7 || rhs.size() < 7) {
    //std::cerr << "Error! Vector size too small!\n";
    sprintf(gaibu_debug_msg,"Vector size error! Could not compare OV Hits");
    gaibu_msg(MERROR,gaibu_debug_msg);
    printf("Vector size error! Could not compare OV Hits\n");
    exit(1);
  }
  /*
     if( lhs.size() < 7 || rhs.size() < 7) {
     std::cerr << "Error! Vector size too small!\n";
     sprintf(gaibu_debug_msg,"Error! Vector size too small!");
     gaibu_msg(MWARNING, gaibu_debug_msg);
     exit(1);
     }
     */
  long int dt_high=( (lhs[1]<<8) + lhs[2] - (rhs[1]<<8) - rhs[2] );
  long int dt_low=( (lhs[3]<<8) + lhs[4] - (rhs[3]<<8) - rhs[4] );
  long int dt_16ns_high=( lhs[5] - rhs[5] );
  long int dt_16ns_low=( lhs[6] - rhs[6] );

  if(dt_high!=0) return dt_high<0; // Very different timestamps
  else if(dt_low*dt_low>1) return dt_low<0; // Timestamps are not adjacent
  else if(dt_16ns_high*dt_16ns_high>4000000) return dt_16ns_high>0; // Was sync pulse 2sec (sqrd)
  else if(dt_16ns_high!=0) return dt_16ns_high<0; // Order hi 16 bits of clock counter
  else return dt_16ns_low<(0-ClockSlew); // Order lo 16 bits of clock counter
}

inline int GetDir(std::string dir, std::vector<std::string> &myfiles, int opt = 0, int opt2 = 0)
{
  DIR *dp;
  struct dirent *dirp;
  std::string myfname;//,old_fname,new_fname;

  errno = 0;
  if((dp = opendir(dir.c_str())) == NULL) {
    closedir(dp);
    return errno;
  }

  int counter = 0;
  do{
    if((dirp = readdir(dp)) != NULL) {
      counter++;
      myfname=std::string(dirp->d_name);
      if(myfname.find(".") == myfname.npos || (opt && myfname.size() > 2)){// extensions reserved
        if( opt2 || ( (myfname.find("baseline") == myfname.npos) && (myfname.find("processed") == myfname.npos) ) ) {
          //std::cout << myfname << std::endl;
          myfiles.push_back(myfname);
        }
      }
    }
  } while(dirp!=NULL);

  if(closedir(dp)<0) {
    return errno;
  }

  dp=NULL;

  if(myfiles.size()==0)
    return -1;
  else
    return 0;
}

#endif
