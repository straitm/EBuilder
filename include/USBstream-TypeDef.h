#ifndef __USBStreamTypeDef__
#define __USBStreamTypeDef__

#include<vector>

typedef std::vector<int> DataPacket;
typedef std::vector<std::vector<int> > DataVector;
typedef std::vector<int>::iterator DataPacketIt;
typedef std::vector<std::vector<int> >::iterator DataVectorIt;
typedef std::pair<char,short int> OVSignal;

#endif
