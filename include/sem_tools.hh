
#ifndef sem_tools_hh
#define sem_tools_hh

#include <stdio.h> 
#include <cstdlib> 
#include <sys/ipc.h>     /* general SysV IPC structures         */
#include <sys/sem.h>	 /* semaphore functions and structs.    */
#include <iostream>

#include <exception>

union semun {
  int val;
  struct semid_ds *buf;
  unsigned short *array;
}; //arg;


class sem_inc_except: public std::exception
{
  virtual const char* what() const throw()
  {
    return "Can't increase semaphore !!!";
    
  }
} ;

class sem_dec_except: public std::exception
{
  virtual const char* what() const throw()
  {
    return "Can't decrease semaphore !!!";
    
  }
} ;

class sem_dest_except: public std::exception
{
  virtual const char* what() const throw()
  {
    return "Can't destroy semaphore !!!";
  }
} ;

class sem_get_val_except: public std::exception
{
  virtual const char* what() const throw()
  {
    return "Can't get semaphore value !!!";
  }
} ;

static sem_inc_except semincexcept;;

static sem_dec_except semdecexcept;;

static sem_dest_except semdestexcept;

static sem_get_val_except semgetvalexcept;


/* Generate unique key */

inline key_t get_sem_key(int id, std::string nfile = "/etc/hosts"){
  
  key_t key = ftok(nfile.c_str(), id);
  
  if (key==-1) {
    
    perror("ftok: get_sem_key"); exit(1);}

  return key;

}


/* create a private semaphore set with one semaphore in it, */
/* with access only to the owner.                           */

inline int create_semaphore(int id, std::string nfile="/etc/hosts"){

  key_t key = get_sem_key(id, nfile);

  int sem_set_id = semget(key, 1, IPC_CREAT | 0600);
  
  int n_tries = 0; 
  
  while (sem_set_id == -1 && n_tries<30) { // kind of nasty, but works
    
    key = get_sem_key(id + n_tries, nfile);

    sem_set_id = semget(key, 1, IPC_CREAT | 0600); n_tries++;}
 
  if (sem_set_id == -1){ perror("semget: create_semaphore"); exit(1); }

  return sem_set_id;
}

//inline int create_semaphore(int id, std::string nfile="/etc/hosts"){

//  key_t key = get_sem_key(id, nfile);

//  int sem_set_id = semget(key, 1, IPC_CREAT | 0600);

//  if (sem_set_id == -1) {perror("semget: create_semaphore"); exit(1);}
 
//  return sem_set_id;
//}

/* Initilaize semaphore to 1 */

inline void init_semaphore(int sem_id, int val){
  
  union semun arg;
    
  arg.val = val;

  int ok = semctl(sem_id,0,SETVAL,arg);
  
  if (ok==-1) { perror("semctl: init_semaphore") ;exit(1); }

  return;
}

/* get semaphore value */

inline int get_semaphore_value(int sem_id){
  
  union semun arg;
  
  int sem_value; 

  if( (sem_value = semctl(sem_id,0,GETVAL, arg)) ==-1 ) throw semgetvalexcept;

  return sem_value;

}

/* destroy semaphore */

inline void destroy_semaphore(int sem_id){
  
  union semun arg;
  
  //if (semctl(sem_id,0,IPC_RMID, arg) == -1) { 

  //perror("semctl: destroy_semaphore"); exit(1); }

  if( semctl(sem_id,0,IPC_RMID, arg) ==-1 ) throw semdestexcept;

  return;

}

inline void increase_semaphore(int sem_id){
  
  struct sembuf sem_op;

  /* wait on the semaphore, unless it's value is non-negative. */
  sem_op.sem_num = 0;
  sem_op.sem_op =  1;   /* ! */
  sem_op.sem_flg = 0;
  int ok = semop(sem_id, &sem_op, 1);
  
  if (ok==-1) throw semincexcept;
    
  return;

}

inline void decrease_semaphore(int sem_id){
  
  struct sembuf sem_op;

  /* wait on the semaphore, unless it's value is non-negative. */
  sem_op.sem_num = 0;
  sem_op.sem_op = -1;   /* ! */
  sem_op.sem_flg = 0;
  int ok = semop(sem_id, &sem_op, 1);
    
  if (ok==-1) throw semdecexcept;
    
  return;

}

inline void lock_semaphore(int sem_id){
  
  decrease_semaphore(sem_id);

}

inline void unlock_semaphore(int sem_id){
  
  increase_semaphore(sem_id);

}

#endif
