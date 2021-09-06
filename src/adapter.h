#pragma once

#include <hdfs/hdfs.h>




/**
 * hdfsEncryptionZoneInfo- Information about an encryption zone.
 */
typedef struct {
    int mSuite; /* the suite of encryption zone */
    int mCryptoProtocolVersion; /* the version of crypto protocol */
    int64_t mId; /* the id of encryption zone */
    char * mPath; /* the path of encryption zone */
    char * mKeyName; /* the key name of encryption zone */
} hEZI;
