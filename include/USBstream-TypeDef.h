// Size of an ADC "packet" (glittery quotation marks) array after
// USBStream::decode(), with exactly 1 hit.
const unsigned int MIN_ADC_PACKET_SIZE = 7;

typedef std::vector<std::vector<uint16_t> > DataVector;
