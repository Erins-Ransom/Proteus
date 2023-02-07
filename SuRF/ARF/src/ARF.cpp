// Andrew
// #include "ARF.h"
#include "../include/ARF.h"

using namespace arf;

void ARF::set_used(node * n, int used_value) {
    n->used_counter = used_value;
}

void ARF::modify_used(node *n, int to_add) {
    //! to_add can also be negative
    if(is_training_phase) {
        n->used_counter+=to_add;
    } else {
        if( n->leaf_value == true || space_of_counters ==0)
            return;

        if(to_add<0)
            n->used_counter = 0;
        if(to_add == 0)
            return;
        if(to_add>0)
            n->used_counter = 1;
    }
}

void ARF::sanity_check(node * n) {
    if(n->is_leaf) {
        assert_is_leaf(n);
    } else {
        assert_children_integrity(n);
        sanity_check(n->left_child);
        sanity_check(n->right_child);
    }
}

void ARF::sanity_check() {
    sanity_check(root);
}

int ARF::get_used(const node& n) {
    assert(n.is_leaf);

    if(is_training_phase) {
        return n.used_counter;
    } else {
        assert(space_of_counters == 0 || space_of_counters == 1);

        if(n.leaf_value == true) {
            return 0;
        } else {
            if(space_of_counters == 0)
                return 0;
            else
                return (n.used_counter>0);
        }
    }
}

void ARF::learn_from_tn(uint64 left, uint64 right) {
    if(is_training_phase == false && space_of_counters == 0)
        return;
    increment_used(root,left,right,1);
}

void ARF::increment_used(node * n, uint64 left, uint64 right, int to_add) {
    node * leaf_node;
    navigate_internal(n,left,&leaf_node);
    while( leaf_node->left<=right) {
        // increment used hiar //
        modify_used(leaf_node, to_add);
        if(leaf_node->right == this->getDomain() || leaf_node->right == right)
            break;
        navigate_internal(n,leaf_node->right+1,&leaf_node);
        assert(leaf_node->left >= left);
    }
}

void ARF::perfect(Database * database)
{
    Query::Query_t q;
    q.left = 0;
    q.right = this->getDomain();
    vector<Query::Query_t> empty = database->determineEmptyRanges(q);
    for(int i = 0;i<empty.size();i++) {
        learn_from_fp(empty[i].left,empty[i].right);
    }
}

bool ARF::navigate(uint64 left, uint64 right) {
    return navigate(root,left,right);
}

void ARF::learn_from_fp(uint64 left, uint64 right) {
    escalate(left,true);
    escalate(right,false);
    mark_range(root,left,right,false);
}

uint64 ARF::getDomain() {
    return root->right;
}

void  ARF::print_node(node * n) {
    cout<<"[ "<<n->left<<" - "<<n->right<<" ]"<<endl;
}

void ARF::mark_range(node * n, uint64 left, uint64 right, bool value) {
    // precond: called after escalate has been called, ie, the range exists perfectly
    node * leaf_node;
    navigate_internal(n,left,&leaf_node);
    while( leaf_node->right<=right) {
        leaf_node->leaf_value = value;
        if(leaf_node->right == this->getDomain() || leaf_node->right == right)
            break;
        navigate_internal(n,leaf_node->right+1,&leaf_node);
        assert(leaf_node->left >= left);
    }
}

// ./Experiment9.out 24 100000 0 8 2> graph13new.txt

void ARF::escalate(uint64 bound, bool is_left, bool target_value) {
    node * n = NULL;
    navigate_internal(root,bound,&n);
    assert(n!=NULL);
    assert_is_leaf(n);
    assert(contains(n,bound));
    if(verbose) {
        if(is_left)
            cout<<"left bound:"<<bound<<endl;
        else
            cout<<"right bound:"<<bound<<endl;
    }
    // create the leaf within :-)
    if((is_left && n->left == bound) || (!is_left && n->right == bound)
       || n->leaf_value == target_value) {
        // there is no need to split, we are in the clear
        return;
    }

    while(n->leaf_value != target_value) {
        if(verbose == true) {
            printf("splitting node");
            print_node(n);
        }

        split(n);

        if(is_left && n->right_child->left == bound)
            return;
        if(!is_left && n->left_child->right == bound)
            return;
        if(contains(n->left_child,bound)) {
            n = n->left_child;
        } else {
            assert(contains(n->right_child,bound));
            n = n->right_child;
        }
    }
}

void ARF::assert_is_leaf(node * n) {
    assert(n->left_child == NULL);
    assert(n->right_child == NULL);
    assert(n->is_leaf == true);
}

uint64 ARF::get_middle(uint64 l, uint64 r) {
    return (r+l)>>1;
}

ARF::~ARF() {
    free_nodes(root);
}

void ARF::free_nodes(node * n) {
    if(n == NULL)
        return;
    if(n->is_leaf) {
        free(n);
        return;
    }
    free_nodes(n->left_child);
    free_nodes(n->right_child);
    free(n);
}

void ARF::split(node * n) {
    assert_is_leaf(n);
    sanity_check(n);
    node * l = (struct node*) malloc(sizeof(node));
    node * r = (struct node*) malloc(sizeof(node));
    int previous_space = space_of_node(n);

    uint64 middle = get_middle(n->left,n->right);
    bool left_value = db->rangeQuery(n->left,middle);
    bool right_value = db->rangeQuery(middle+1,n->right);

    if(verbose)
        cout<<n->left <<" "<<middle<<" "<<right<<endl;

    initialize_node(l,n->left,middle,true,left_value, n->used_counter,NULL,NULL,n);
    initialize_node(r,middle+1,n->right,true,right_value, n->used_counter,NULL,NULL,n);
    initialize_node(n,n->left,n->right,false,false,0,l,r,n->parent);
    assert(!n->is_leaf);

    // update size counters
    this->size -= previous_space;
    sanity_check(n);
}

int ARF::space_of_node(node * n)
{
    int s = 0;
    if(n->is_leaf) {
        s = 1; //for the existence bit and the bit in the tree signifying it's a leaf
        if(n == root)
            s = 2;
        if(n->leaf_value == false) { //we maintain used counters for empty leaves (ie the ones guarding against false positives)
            s+= space_of_counters;
        }
    } else {
        s = 2; //for the two bits used to represent the children
    }
    return s;
}

void ARF::initialize_node(node * n,uint64 left, uint64 right,bool is_leaf,
                          bool leaf_value,int used_counter,
                          node * left_child, node * right_child, node * parent) {
    n->left = left;
    n->right = right;
    n->is_leaf = is_leaf;
    n->used_counter = used_counter;
    n->left_child = left_child;
    n->right_child = right_child;
    n->parent = parent;
    n->leaf_value = leaf_value;
    this->size+= space_of_node(n);
}

void ARF::end_training_phase() {
    this->is_training_phase = false;
    this->stats.reset();
}

void ARF::reset_training_phase() {
    this->is_training_phase = true;
    this->stats.reset();
}

void ARF::print_size(node * n, int& nodes, int& leaves) {
    if(n->is_leaf) {
        leaves++;
        //print_node(n);
        return;
    } else {
        nodes+=2;
        print_size(n->left_child, nodes, leaves);
        print_size(n->right_child, nodes, leaves);
        return;
    }
}

void ARF::print_size() {
    int nodes = 0;
    int leaves = 0;
    print_size(root, nodes, leaves);
    printf("Size: %d \n", this->size);
    printf("Leaves: %d \n", leaves);
    printf("Nodes: %d \n", nodes);
}

ARF::ARF(int space_of_counters, uint64 domain, Database * db) {
    this->space_of_counters = space_of_counters;
    this->size = 0;
    this->is_training_phase = true;
    this->db = db;
    this->root = (struct node*) malloc(sizeof(struct node));
    this->stats = Statistics();
    this->verbose = false;

    initialize_node((this->root),0,domain,true,true,0,NULL,NULL,NULL);
    this->size = space_of_node(root);
    assert(root->leaf_value == true);
    last_evicted_left = 0;
    last_evicted_right = 0;
    start_over = true;
    this->last = NULL;
    this->merged = 0;
}

bool ARF::contains(node * n, uint64 key) {
    return (n->left<=key && n->right>=key);
}

void ARF::assert_children_integrity(node * n) {
    assert(n->left_child!=NULL && n->right_child!=NULL);
    assert(n->left_child->right == n->right_child->left -1);
    assert(n->left_child->right>= n->left_child->left);
    assert(n->right_child->right>= n->right_child->left);
    assert(n->left_child->parent == n && n->right_child->parent == n);
}

ARF::node * ARF::getLeaf(uint64 key) {
    node * res;
    navigate_internal(root,key,&res);
    return res;
}

bool ARF::navigate_internal(node * n, uint64 key, node ** leaf_node) { //point query
    assert(n->left<=key && n->right>=key);
    //print_node(n);

    if(n->is_leaf) {
        *leaf_node = n;
        return n->leaf_value;
    } else {
        assert_children_integrity(n);
        if(contains(n->left_child,key)) {
            return navigate_internal(n->left_child,key,leaf_node);
        } else {
            return navigate_internal(n->right_child,key,leaf_node);
        }
    }
}

bool ARF::navigate(node *n, uint64 left, uint64 right) {
    node * leaf_node;
    bool result = navigate_internal(n,left,&leaf_node);
    while(result==false && leaf_node->right<right) {

        result = result | navigate_internal(n,leaf_node->right+1,&leaf_node);

    }
    return result;
}


bool ARF::handle_query(uint64 left, uint64 right,bool actual_result,  bool do_adapt) {
    bool arf_result = navigate(root, left,right);
    if (do_adapt) {
        if (arf_result == true && actual_result == false) {
            learn_from_fp(left, right);
            assert(navigate(root, left, right) == false);
        }
        if (arf_result == true && actual_result == true) {
            learn_from_tp(left, right);
        }
        if (arf_result == false && actual_result == false) {
            learn_from_tn(left, right);
        }
    }
    assert(!(arf_result == false && actual_result ==true));
    this->stats.update(arf_result, actual_result);
    return arf_result;
}

void ARF::learn_from_tp(uint64 left, uint64 right) {
    //TODO write me -- optional
}

void ARF::evict(node * n) {
    assert(is_logical_node(*n));
    // cout<<"evicting:"<<n->left<<"-"<<n->right<<endl;

    if((get_used(*(n->left_child))>0 || get_used(*(n->right_child))>0) &&
       (n->left_child->leaf_value != n->right_child->leaf_value) ) {
        modify_used(n->left_child, -1);
        modify_used(n->right_child, -1);
        if(space_of_counters ==0 && is_training_phase == false)
            assert(1==0);
    } else {
        if(verbose)
            cout<<"prev:"<<this->size<<endl;
        merge(n->left_child, n->right_child);
        merged++;
        if(verbose)
            cout<<"after:"<<this->size<<endl;
    }
    last_evicted_left = n->left;
    last_evicted_right = n->right;
    last = n;
}

bool ARF::is_logical_node(const node & n) {
    return (!n.is_leaf && n.left_child->is_leaf && n.right_child->is_leaf);
}

bool ARF::found_last_evicted(node * n) {
    assert(n!=NULL);
    if(n->left>= last_evicted_left || start_over)
        return true;

    //assert(1==0);
    return false;
}

void ARF::truncate(node * n, int target, bool * skipped_last) {
    // cout<<"looking at node "<<n->left<<"-"<<n->right<<endl;
    if(this->size<= target || n == NULL)
        return;

    if(found_last_evicted(n))
        *skipped_last = true;

    if(n->is_leaf) {
        modify_used(n, -1);
        return;
    }

    if(*skipped_last && is_logical_node(*n)) {
        evict(n);
        return;
    }
    truncate(n->left_child, target, skipped_last);
    truncate(n->right_child, target, skipped_last);
}

void ARF::recordNewKeys(uint64 * keys,int num_keys,int strategy) {
    for(int i=0; i<num_keys;i++) {
        node * n = NULL;
        navigate_internal(root,keys[i],&n);
        n->leaf_value = true;
        /*
          escalate(keys[i],true, true);
          escalate(keys[i],false, true);
          mark_range(root,keys[i],keys[i],true);
          assert(navigate(keys[i],keys[i]) == true);*/
    }
}

void ARF::truncate(int target) {
    int loops = 0;
    bool skipped_last = false;
    merged = 0;
    /*  cout<<"TRUNCAET STARTED"<<endl;
        if(last!=NULL)
        cout<<"last:"<<last->left<<"-"<<last->right<<endl;*/
    if(verbose)
        cout<<"Initial size:"<<size<<endl;
    while (this->size > target) {
        truncate(root, target, &skipped_last);
        loops++;
        // meta tin prwti fora, i ftasame merhi simeio X kai eimaste entaksei
        // i to eidame olo (kai eite eimaste entaksei eite oxi,
        //to thema einai oti pame apo tin arxi meta)
        if(this->size > target) {
            start_over = true;
        } else {
            start_over = false;
        }
    }
    //printf("Truncate loops for fast synopsis: %d \n", loops);
    if(verbose)
	printf("Merged for fast synopsis: %d \n", merged);
    //getchar();
    //printf("Truncate loops for fast synopsis: %d \n", loops);
}

void ARF::merge(node * left, node * right) { //free?
    node * parent = left->parent;
    sanity_check(parent);
    assert(left->parent == right->parent);
    assert(!parent->is_leaf);

    int previous_space = space_of_node(left)+space_of_node(right)+space_of_node(parent);

    parent->is_leaf = true;

    parent->left_child = NULL;
    parent->right_child = NULL;
    // space and used counters

    bool value = left->leaf_value | right->leaf_value;
    parent->leaf_value = value;

    if(value == false )
        parent->used_counter = left->used_counter + right->used_counter;
    else
        parent->used_counter = 0;

    free(left);
    free(right);

    this->size -= previous_space - space_of_node(parent);
    sanity_check(parent);
}
