#ifndef ARF_H_
#define ARF_H_

#include  <assert.h>
#include <cstddef> //for NULL
#include "Database.h"
#include <stdlib.h>
#include "Statistics.h"

using namespace std;

namespace arf {

class ARF {
 public:
    struct node {
        uint64 left;
        uint64 right;
        bool is_leaf;
        bool leaf_value;
        int used_counter;
        struct node * left_child;
        struct node * right_child;
        struct node * parent;
    };

    int space_of_counters; //0 for round robin, 1 for 1-used bit
    struct node * root;
    struct node * last;
    bool is_training_phase;
    Database * db;
    int size;
    Statistics stats;
    bool verbose;
    uint64 last_evicted_left;
    uint64 last_evicted_right;
    bool start_over;
    int merged;

    ARF();
    virtual ~ARF();
    void free_nodes(node * n);
    node * getLeaf(uint64 key);
    void perfect(Database *db);
    void print_size();
    void print_size(node * n, int& nodes, int& leaves);

    void set_used(node * n, int used_value);
    void modify_used(node *n, int to_add);
    void increment_used(node * n, uint64 left, uint64 right, int to_add);
    int get_used(const node& n);
    void learn_from_tn(uint64, uint64);
    void learn_from_fp(uint64 left, uint64 right);
    void escalate(uint64 bound, bool is_left, bool target_value = false) ;
    void assert_is_leaf(node * n) ;
    uint64 get_middle(uint64 l, uint64 r);
    void split(node * n);
    void sanity_check(node * n);
    void sanity_check();
    int space_of_node(node * n);
    void initialize_node(node * n,uint64 left, uint64 right,bool is_leaf,
                         bool leaf_value,int used_counter, node * left_child, node * right_child, node * parent);
    void end_training_phase();
    void reset_training_phase();
    ARF(int space_of_counters, uint64 domain, Database * db);
    bool contains(node * n, uint64 key) ;
    void assert_children_integrity(node * n);
    bool navigate_internal(node * n, uint64 key, node ** leaf_node);
    void mark_range(node * n, uint64 left, uint64 right, bool value);
    bool navigate(node *n, uint64 left, uint64 right);
    bool navigate(uint64 left, uint64 right);
    bool handle_query(uint64, uint64,bool actual_result, bool do_adapt = true);
    void merge(node * left, node * right);
    void learn_from_tp(uint64 left, uint64 right);
    uint64 getDomain();
    void print_node(node *);

    void truncate(int target);
    void truncate(node * n, int target, bool*);
    bool found_last_evicted(node * n);
    bool is_logical_node(const node & n);
    void evict(node * n);
    void recordNewKeys(uint64 * keys,int num_keys,int strategy);
};

} // namespace arf

#endif /* ARF_H_ */
