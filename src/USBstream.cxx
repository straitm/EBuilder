#include "USBstream.h"

//______________________________________________________

USBstream::USBstream()
{
  mythresh=0;
  myusb=-1;
  //mynpmt=1;
  //myoffset = new int[mynpmt];
  //mypmt = new int[mynpmt];
  mucounter=0;
  spycounter=0;
  avglength=0;
  mytolsp=0;
  mytolutc=0;
  words = 0;
  got_hi = false;
  restart = false;
  first_packet = false;
  time_hi_1 = 0;
  time_hi_2 = 0;
  time_lo_1 = 0;
  time_lo_2 = 0;
  timestamps_read = 0;
  word_index = 0;
  word_count[0] = 0;
  word_count[1] = 0;
  word_count[2] = 0;
  word_count[3] = 0;
  bytesleft=0;
  fsize=0;
  //myvec = new std::vector<std::vector<int> >;
  myvec.reserve(65535);
  myit=myvec.begin();
  IsOpen = false;
  IsFanUSB = false;
  BothLayerThresh = false;
  UseThresh = false;
  for(int i = 0; i < 32; i++) { // Map of adjacent channels
    adj1[i] = i+32;
    if(i==0) adj2[i] = adj1[i];
    else if(i % 8 == 0) adj2[i] = adj1[i]-1;
    else if(i % 8 < 4) adj2[i] = adj1[i]+3;
    else adj2[i] = adj1[i]-4;
    //printf("Adj1[%d]: %d\tAdj2[%d]: %d\n",i,adj1[i],i,adj2[i]);
  }
}

USBstream::~USBstream()
{
  //delete [] myoffset;
  //myoffset = NULL;
  //delete [] mypmt;
  //mypmt = NULL;
  //delete myvec;
}

void USBstream::Reset()
{
  words = 0;
  got_hi = false;
  //time_hi_1 = 0;
  //time_hi_2 = 0;
  //time_lo_1 = 0;
  //time_lo_2 = 0;
  timestamps_read = 0;
  word_index = 0;
  word_count[0] =  0;
  word_count[1] = 0;
  word_count[2] = 0;
  word_count[3] = 0;
  bytesleft=0;
  fsize=0;
}

void USBstream::SetOffset(int *off)
{
  for(int i = 0; i < 64; i++) {
    if(*(off+i) > 0) {
      offset[i] = *(off+i);
    } else {
      offset[i] = 0;
    }
  }
}

void USBstream::SetThresh(int thresh, int threshtype)
{
  //threshtype: 0=NONE, 1=OR, 2=AND
  UseThresh = (bool)threshtype;
  BothLayerThresh=(bool)(threshtype-1);
  if(thresh)
    mythresh=thresh;
  else
    mythresh = -20; // Put SW threshold well below HW threshold (including spread)
}

void USBstream::SetBaseline(int **baseptr)
{
  for(int i = 0; i < 64; i++) {
    for(int j = 0; j < 64; j++) {
      if(*(*(baseptr+i)+j) > 0) {
        baseline[i][j] = *(*(baseptr+i)+j);
      }
      else {
        baseline[i][j] = 0;
      }
    }
  }
  //myvec.clear();
  //myit=myvec.end();
}

bool USBstream::GetBaselineData(DataVector *vec)
{
  vec->clear();
  for(myit = myvec.begin(); myit != myvec.end(); myit++) {
    if(myit->size() > 7) {
      vec->push_back(*myit);
    }
  }
  mytolutc = mytolsp = 0; // Reset mytolutc for data
  time_hi_1 = time_hi_2 = 0;
  time_lo_1 = time_lo_2 = 0;
  return true;
}

bool USBstream::GetNextTimeStamp(DataVector *vec)
{
  unsigned long int tmp = 0;
  while(1) {
    if(myit==myvec.end())
      return false;

    vec->push_back(*myit);

    // Begin Diagnostics
    ++mucounter;
    //if( (*myit).size() ) {
    if( ( ((*myit)[0] >> 8) & 0x7f) % 10 == 2 ) {
      if( ( ((*myit)[0] >> 8) & 0x7f) % 20 != 12  || (myusb != 18 && myusb != 19) ) {
        avglength = (avglength*(float)spycounter + (float)((*myit)[0] & 0xff))/(float)(spycounter+1);
        ++spycounter;
      }
    }
      //}
    // End Diagnostics

    if((*myit).size()>4) { // Look for next time stamp and break
      tmp = (*myit)[1] << 8;
      if(tmp + (*myit)[2] > ((mytolutc >> 16) & 65535)) {
        break;
      }
      else if(tmp + (*myit)[2] == ((mytolutc >> 16) & 65535)) {
        tmp = (*myit)[3] << 8;
        if(tmp + (*myit)[4] > (mytolutc & 65535)) {
          break;
        }
      }
    }
    myit++;
  }
  tmp = ((*myit)[1] << 8) + (*myit)[2];
  mytolutc = (tmp << 16) + ((*myit)[3] << 8) + (*myit)[4];
  //mytolutc = ( ((*myit)[1] << 24) + ((*myit)[2] << 16) + ((*myit)[3] << 8) + (*myit)[4] );
  myit++;

  // Print Diagnostic info
  printf("Number of muon events found in usb stream %d: %ld (%ld) (%f)\n",myusb,mucounter,spycounter,avglength);
  mucounter = 0;
  spycounter = 0;
  avglength = 0;

  return true;
}

//unsigned long int ti) {
int USBstream::LoadFile(std::string nextfile)
{
  sprintf(myfilename,"%s_%d",nextfile.c_str(),GetUSB());
  //printf("%s_%d\n",nextfile.c_str(),GetUSB());

  struct stat myfileinfo;
  if(IsOpen == false) {
    myFile = new fstream(myfilename, fstream::in | fstream::binary);
    if(myFile->is_open()) {
      if(stat(myfilename, &myfileinfo) == 0) { //get file size
        if(myfileinfo.st_size) { IsOpen = true; return 1; }
      }
      myFile->close(); delete myFile; // Clean up
      char msg_buf[100];
      sprintf(msg_buf,"USB %d has died. Exiting.",myusb);
      gaibu_msg(MERROR,msg_buf);
      printf("USB %d has died. Exiting.\n",myusb);
      return -1;
    }
    else { delete myFile; }
  }
  else return 1;

  std::cout << "Waiting for files...\n";
  return 0;
}

bool USBstream::decode()
{
  char filedata[65535];//data buffer

  if(!myFile->is_open()) {
    std::cerr << "File not open! Exiting.\n";
    exit(1);
    return false;
  }

  if(myit==myvec.end()) {
    myvec.clear();
  }
  else if(myit<myvec.end()) {
    myvec.assign(myit,myvec.end());
    //myvec.clear();
  }

  Reset();

  if(stat(myfilename, &fileinfo) == 0) //get file size
    bytesleft = fileinfo.st_size;

  unsigned int long word = 0; // holds 24-bit word being built, must be unsigned int
  char exp = 0; // expecting this type next
  //char byte[1];
  int counter=0;

  while(true){ //loop over buffer packets
    counter++;
    if(bytesleft < 65535)
      fsize = bytesleft;
    else {
      fsize = 65535;
      bytesleft -= 65535;
    }

    myFile->read(filedata, fsize);//read data of appropriate size

    for(int bytedex = 0; bytedex < fsize; bytedex++){ //loop over members in buffer
      //char payload = *bytes.byteme & 63;
      //char type = *bytes.byteme >> 6;
      //printf("byte = %b\n",*byte);
      char payload = filedata[bytedex] & 63;
      char pretype = (filedata[bytedex] >> 6) & 3;
      //printf("payload = %b\n",payload);
      //printf("type = %b\n",pretype);
      if(pretype == 0){ //not handling type very well.
        //cout << "type == 0" << endl;
        exp = 1;
        word = payload;
      }
      else {
        if(pretype == exp) {
          //cout << "type == exp" << endl;
          word = (word << 6) | payload; //bitwise OR
          if(++exp == 4) {
            //cout << "++exp == 4" << endl;
            exp = 0;

            if(got_word(word)) { //24-bit word stored, process it
              restart = false;
              myit=myvec.end();
              myFile->seekg(std::ios::beg);
              data.clear();
              return false;
            }
          }
        }
        else {
          char msg_buf[100];
          sprintf(msg_buf,"Found corrupted data in file %s",myfilename);
          gaibu_msg(MERROR,msg_buf);
          printf("Found corrupted data in file %s\n",myfilename);
          exp = 0;
        }
      }
      //myFile.seekg(1, ios_base::cur);
    }
    if(fsize==bytesleft) //exit the loop after last read
      break;
  }
  //extra.push_back(data); // Uncommenting these 2 lines assumes OVDAQ only writes complete packets to disk at start/end of files
  //flush_extra();         // For now this is not true;

  //std::cout << "File manipulations\n";  std::cout << "Closing files\n";
  while(myFile->is_open()) {
    myFile->close();
    usleep(100);
  }
  delete myFile;
  IsOpen = false;

  myit=myvec.begin();
  return true;
}

bool USBstream::got_word(unsigned long int d)
{
  //cout << "got_word called" << endl;
  words++;
  char type = (d >> 22) & 3;
  if(type == 1) { // command word, not data
    //unsigned int b1 = (d >> 16) & 63;
    //unsigned int b2 = (d >> 8) & 255;
    //unsigned int b3 = d & 255;
    flush_extra();
    //print "c,$b1,$b2,$b3\n";
  }
  else if(type == 3) {
    if(check_debug(d))
      return restart;

    data.push_back((unsigned int)(d & 65535));//segfault this line
    //printf("top of data = %b\n",data.front());
    check_data();
  }
  else {
    //printf "?,%06x\n", d; //another info type
  }
  return 0;
}

void USBstream::check_data()
{
  //cout << "check_data called" << endl;
  while(1) {
    //cout << data.size() << endl;
    bool got_packet = false;
    unsigned int got = data.size();

    if(!got)
      break;
    if(data[0] == 0xffff) { // check header
      //std::cout << "proper header" << std::endl;
      if(!(got >= 2))
        break;
      //data.pop();
      unsigned int len = data[1] & 255;
      //const unsigned int lenless = len-1;
      if(len > 1) {
        //std::cout << "proper len" << std::endl;
        //cout << got << endl;
        //cout << len+1 << endl;
        if(!(got >= len + 1))
          break;
        //std::cout << "big enough" << std::endl;
        unsigned int par = 0;
        //int packet[lenless];
        int module = (data[1] >> 8) & 0x7f;
        int type = data[1] >> 15; // check to see if trigger packets
        //printf("Module: %d\n",module);
        //printf("Type: %d\n",type);
        std::vector<int> *packet=new std::vector<int>;
        bool hitarray1[64] = {0};
        bool hitarray2[64] = {0};

        for(unsigned int m = 1; m <= len; m++) {
          par ^= data[m];
          //printf("par = %b\n",par);
          if(m<len) {
            if(m<4) {
              // Handle trigger box packets
              if(m==3 && IsFanUSB) {
                if(module >= 0 && module <= 63) {
                  int low = (data[m] & 0xffff) - offset[module];
                  if(low < 0) {
                    int high = packet->back() - 1;
                    if(high < 0) { high += (1 << 13); } // sync packets come every 2^29 clock counts
                    packet->pop_back(); packet->push_back(high);
                    low += (1 << 16);
                    packet->push_back(low);
                  }
                  else { packet->push_back(low); }
                }
                else { packet->push_back(data[m]); }
              }
              else {
                packet->push_back(data[m]);
              }
            }
            else {
              if(type) {
                if(m%2==0) { // ADC charge
                  if(data[m+1]>63 || data[m+1]<0 || module<0 || module>63) {
                    /*
                       sprintf(gaibu_debug_msg,"Error out of range! Module: %d\tChannel: %d",module,data[m+1]);
                       gaibu_msg(MERROR,gaibu_debug_msg);
                       printf("Error out of range! USB: %d\tModule: %d\tChannel: %d\n",myusb,module,data[m+1]);
                       */
                  }
                  else {
                    //if(data[m+1]<=63 && data[m+1]>=0 && module>=0 && module<=63) \{
                    data[m] -= baseline[module][data[m+1]];
                    packet->push_back(data[m]);
                    packet->push_back(data[m+1]);
                    hitarray1[data[m+1]] = true;
                    if(data[m]>mythresh) {
                      hitarray2[data[m+1]] = true;
                    }
                  }
                }
              }
              else { packet->push_back(data[m]); }
            }
          }
          if(m==1) {
            packet->push_back(time_hi_1);
            packet->push_back(time_hi_2);
            packet->push_back(time_lo_1);
            packet->push_back(time_lo_2);
          }
        }

        //cout << par << endl;
        if(!par) { // check parity
          //cout << "proper parity" << endl;
          got_packet = true;
          flush_extra();
          if(first_packet) { first_packet = false; }

          // This block builds muon events
          bool MuonEvent = false;
          if(UseThresh && type) {
            if(BothLayerThresh) {
              for(int i = 0; i<32; i++) {
                if( hitarray2[i]) {
                  if( hitarray2[adj1[i]] || hitarray2[adj2[i]] ){ MuonEvent = true; break; }
                }
              }
            }
            else {
              for(int i = 0; i<32; i++) {
                if(hitarray1[i]) {
                  if( hitarray2[adj1[i]] || hitarray2[adj2[i]] ){ MuonEvent = true; break; }
                }
                if(hitarray2[i]) {
                  if( hitarray1[adj1[i]] || hitarray1[adj2[i]] ){ MuonEvent = true; break; }
                }
              }
            }
          }
          else {
            /*
               if(UseThresh && !type) { // can only apply threshold to ADC packets
               sprintf(gaibu_debug_msg,"Threshold requested for trigger packet");
               gaibu_msg(MERROR,gaibu_debug_msg);
               sprintf(gaibu_debug_msg,"Error! Threshold requested for trigger packet\n");
               }
               */
            MuonEvent = true;
          }
          if(packet->size() > 7) { // guaruntees at least 1 hit (size > 9 for 2 hits)
            if(MuonEvent) { // Mu-like double found for this event
              if( myvec.size() > 0 ) { //&& LessThan(*packet,myvec.back()) ) \{
                DataVectorIt InsertionSortIt = myvec.end();
                bool found = false;
                while(--InsertionSortIt >= myvec.begin()) {
                  if(!LessThan(*packet,*InsertionSortIt)) {
                    // Due to edge strip trigger logic in the trigger box firmware,
                    // we find duplicate trigger box packets of the form:
                    // p, 15366, 8086, 6128, 0, 1100 0000 0000 0000
                    // p, 15366, 8086, 6131, 0, 1000 0000 0000 0000
                    // These packets come from the same coincidence and one packet is missing
                    // a hit because its partner was actually also satisfied the mu-like double
                    // criteria.
                    // We search for these duplicate trigger box packets and make an OR of the hit data
                    if(IsFanUSB) {
                      // packets should be separated by no more than 3 clock cycles
                      if(!LessThan(*InsertionSortIt,*packet,3)) {
                        // packets should come from the same module
                        if(((packet->at(0) >> 8) & 0x7f) == ((InsertionSortIt->at(0) >> 8) & 0x7f)) {
                          // Trigger box packets all have a fixed size
                          (*InsertionSortIt)[7] |= (*packet)[7];
                          (*InsertionSortIt)[8] |= (*packet)[8];
                          found = true;
                          break;
                        }
                      }
                    }
                    myvec.insert(InsertionSortIt+1,*packet);
                    found = true;
                    break;
                  }
                }
                if(!found) { myvec.insert(myvec.begin(),*packet); } // Reached beginning of myvec
              }
              else { myvec.push_back(*packet); }
            }
          }
          //myvec.push_back(*packet);
          //proc(packet);
          //delete first few elements of data
          data.erase(data.begin(),data.begin()+len+1); //(no longer)
        }
        else {
          sprintf(gaibu_debug_msg,"Found packet parity mismatch in USB stream %d",myusb);
          //gaibu_msg(MNOTICE,gaibu_debug_msg);
          printf("Found packet parity mismatch in USB stream %d\n",myusb);
        }
        delete packet;
      }
    }
    if(!got_packet){
      extra.push_back(data.front());
      data.pop_front();
    }
  }
}

/*
  # c1 dac pmt
  # c2 dac
  # c5 XXXX  -skipped
  # c6 data_hi    - number of received data words
  # c6 data_lo
  # c6 lost_hi     - number of lost data words
  # c6 lost_lo
  # c8 timestamp_hi
  # c9 timestamp_lo
*/

bool USBstream::check_debug(unsigned long int d)
{
  //cout << "check_debug called..." << endl;
  unsigned int a = (d >> 16) & 255;
  d = d & 65535;

  if(a == 0xc8) {
    time_hi_1 = (d >> 8) & 255;
    time_hi_2 = d & 255;
    got_hi = true;
    return 1;
  }
  else if(a == 0xc9) {
    if(got_hi) {
      time_lo_1 = (d >> 8) & 255;
      time_lo_2 = d & 255;
      got_hi = false;
      timestamps_read++;
      flush_extra();
      if(!mytolutc) { // Check to see if first time stamp found and if so, rewind file
        mytolutc = (time_hi_1 << 8) + time_hi_2;
        mytolutc = (mytolutc << 16) + (time_lo_1 << 8) + time_lo_2;
        restart = true;
        first_packet = true;
      }
      //print "t,$time\n";
    }
    return 1;
  }
  else if(a == 0xc5) {
    word_index = 0;
    return 1;
  }
  else if(a == 0xc6) {
    word_count[word_index++] = d;
    if(word_index == 4) {
      //long unsigned int myshift = 1;
      //myshift = (myshift << 32);
      long int t = (word_count[0] << 16) + word_count[1];
      long int v = (word_count[2] << 16) + word_count[3];
      long int diff = t - v - words;
      if(diff < 0) { diff += (1 << 31); diff += (1 << 31); }
      flush_extra();
      //print "n,$t,$v,$words,$diff\n";
    }
    return 1;
  }
  else if(a == 0xc1) {
    flush_extra();
    //print "dac,$d\n";
    return 1;
  }
  else if(a == 0xc2) {
    flush_extra();
    //print "delay,$d\n";
    return 1;
  }
  else
    return 0;
}

void USBstream::flush_extra()
{
  if(!extra.empty()) {
    extra.clear();
    if(!first_packet && mytolutc) { // Ignore incomplete packets at the beginning of the run
      sprintf(gaibu_debug_msg,"Found x packet in file %s",myfilename);
      //gaibu_msg(MNOTICE,gaibu_debug_msg);
      printf("Found x packet in file %s\n",myfilename);
    }
  }
}
