import rosbag
import csv
import argparse
import os

'''

This script parses .bag files to .csv.

It uses a recursive approach to loop through all the topics and all of its attributes.
For each topic in the .bag file, it creates a new .csv.

'''

# -------------------------
#        LOADS FILE
# -------------------------

DIR = './oneclass_classification/poly_01/simulation_5/'

def loadBag():
    '''
        Loads the bag file to be parsed.
    '''

    #os.mkdir(DIR + '/csvs_1', 0755)
    bag = rosbag.Bag(DIR + 'simulation_5.bag', 'r')
    return bag

# -------------------------
#        GET TOPICS
# -------------------------

def getTopics(bag):
    '''
        Returns all the topics names.
    '''

    return bag.get_type_and_topic_info()[1]

# -------------------------
#        GET FILENAME
# -------------------------

def getFilename(topic):
    '''
        Creates and returns a custom filename (following a template)
        of the csv file to be generated.
    '''

    filename = ''
    for i in range(len(topic)-1, 0, -1):
        if(topic[i] == '/'):
            break
        filename = topic[i] + filename

    return DIR + '/' + filename + '_generated.csv'

# -------------------------
#      GET ATTRIBUTES
# -------------------------

def getAttributes(obj, slots, parents=''):
    '''
        Recursive function which takes as input:

            - obj: the object to retrieve the attributes
            - slots: a list, representing the slots of the object
            - parents: a string representing the objects which contain the attributes (slots) to be analysed

        It returns a list of all the elements to be added to the .csv
    '''
    resAttrs = []

    for attr in slots:
        if isinstance(getattr(obj, attr), (int, float, str, tuple, list)):
            resAttrs.append('%s%s' % (parents, attr))
        else:
            parentsComposition = '%s%s.' % (parents, attr)
            resAttrs = resAttrs + getAttributes(getattr(obj, attr), getattr(obj, attr).__slots__, parentsComposition)

    return resAttrs

# -------------------------
#       GET VALUES
# -------------------------

def getValues(obj, slots):
    '''
        Recursive function which takes as input:

            - parent: an object representing the parent

        It returns a list of all the elements to be added to the .csv
    '''
    values = []

    for attr in slots:
        if isinstance(getattr(obj, attr), (int, float, str, tuple, list)):
            values.append(getattr(obj, attr))
        else:
            values = values + getValues(getattr(obj, attr), getattr(obj, attr).__slots__)

    return values
    
# -----------------------------
#   CREATE CSV FILE FOR TOPIC
# -----------------------------

def createTopicCSVFile(bag, topics, topic_name):
    msg_type = topics[topic_name].msg_type
    topic_msgs = bag.read_messages(topics=topic_name)
    # first message to know its type attributes (all subsequent messages will have these attributes)
    msg = topic_msgs.next().message
    attributes = getAttributes(msg, type(msg).__slots__)

    with open(getFilename(topic_name), 'wb') as csvfile:
        filewriter = csv.writer(csvfile, delimiter=',')
        filewriter.writerow(attributes)

        for single_msg in topic_msgs:
            values = getValues(single_msg.message, type(single_msg.message).__slots__)
            filewriter.writerow(values)
        
    csvfile.close()

# -----------------------------
#             MAIN
# -----------------------------

def main():
    '''
    parser = argparse.ArgumentParser(description='Process the file')

    parser.add_argument('-orig', help='path of the file to be parsed', required=True, type=str)
    parser.add_argument('-dest', help='path of the file to create the generated .csv file', required=True, type=str)

    args = parser.parse_args()

    print(str(args.orig))
    print(str(args.dest))
    '''
    
    bag = loadBag()
    topics = getTopics(bag)

    print('DIR: ', DIR)
    
    for topic_name in topics:
        print('Generating ' + topic_name + '.csv file...')
        createTopicCSVFile(bag, topics, topic_name)
        print('Generation has finished.\n\n')
    


if(__name__ == '__main__'):
    main()
