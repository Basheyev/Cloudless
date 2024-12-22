# Cloudless Knowledge Storage

## 1. Overview
Cloudless is a P2P knowledge storage application that ensures resilient
and accessible knowledge for teams without relying on servers or clouds.
It provides distributed storage (vaults) with features such as peer node
synchronization, hierarchical and referential navigation, access control,
versioning, and markdown-based article creation for neat documentation 
with mutual references and full-text search. An advanced indexing system
automatically builds a knowledge graph to enhance organization and retrieval.
Fully autonomous after initial setup, Cloudless self-replicates, self-updates,
and operates without requiring ongoing IT maintenance. 

## 2. Target audience and use cases

Cloudless target office-based small and medium businesses (10-50 employees),
a market with over 1.5 million businesses worldwide. Cloudless leverages 
end-user office computer hardware to save up to $10 per user per month on 
cloud services and improves employee productivity by 3-5% through easily 
accessible knowledge, provided that employees actively use the articles. 
This results in an economic impact ranging from 9x to 30x. Cloudless 
prioritizes knowledge resilience and accessibility in a cozy and simple UI, 
offering a user experience similar to Google Drive and Obsidian, making it 
intuitive and accessible for non-technical users.

Cloudless knowledge storing use cases:
1. Policies, processes and SOPs.
2. Marketing & Sales Content.
3. Customer Support Documentation.
4. Training and Development.
5. Projects Documentation.


## 3. Workloads assumtions

The baseline assumption is that each peer will create approximately 120 markdown 
articles per year, averaging 50 KB per article, and attach 3-5 files per article
with a total average size of 3 MB including formats such as PNG, JPEG, PDF, XLSX,
DOCX, PPTX, CSV, and MPEG. Files are deduplicated and compressed with a 40-60% 
efficiency using gzip. Reads are expected to exceed writes by a factor of 5. 
For a team of 50 users, the average peer storage requirement is approximately 
10 GB per year.

All public vault articles and attachments are synchronized across all nodes,
assuming each peer has at least 50 GB of free disk space, as a significant 
portion of office computer hard drives often goes unused. Authors can assign
reading and editing rights to other peers. Private or personal articles and
attachments are stored on three other nodes for backup purposes.


## 4. Cloudless modules

1. **Storage** - Virtual file system in a single file, creation/modification/deletion 
of markdown articles, link specification and generation, file attachments, version management,
full-text indexing, stream compression/decompression, basic disaster recovery mechanisms. 

2. **Explorer** - User interface: hierarchical navigation, full-text search, knowledge graph
navigator, editor for articles and files with version control, data viewer, multi-language 
support, auto-updates. Possible stack: Qt.

3. **Security** - Access control, asymmetric keys, authentication of nodes and users 
(names, tokens).

4. **Sync** - Automatic peer discovery, synchronization with policies (public/private data),
access control, data recovery, integrity verification (hashing), multithreading for 
faster data exchange, performance tracking and insights. 