export interface Message {
  text: string;
  sender: 'user' | 'bot';
  timestamp: Date;
}

export interface FolderDetails {
  id: number;
  folder_path: string;
  documents?: Array<any>;
  entities?: Array<any>;
}

export interface FolderConfigurationEntity {
  id: number;
  name: string;
  description?: string;
  type: string;
} 