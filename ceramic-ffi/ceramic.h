
extern "C" {

struct Bytes {
    char* data;
    uint len;
};

Bytes ceramic_kubo_dag_get(char *cid);

}
