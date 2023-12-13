import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
import scipy.stats

class QNetwork(nn.Module):
    def __init__(self, action_dim):
        super(QNetwork, self).__init__()
        self.fc1 = nn.Linear(action_dim, 128)
        self.fc2 = nn.Linear(128, 64)
        self.output_layer = nn.Linear(64, 1)

    def forward(self, action):
        x = torch.relu(self.fc1(action))
        x = torch.relu(self.fc2(x))
        q_value = self.output_layer(x)
        return q_value



class DeepQAgent:
    def __init__(self, action_dim, exploration_coefficient=1.0, learning_rate=0.001, gamma=0.99):
        self.action_dim = action_dim
        self.exploration_coefficient = exploration_coefficient
        self.gamma = gamma

        self.q_network = QNetwork(action_dim)
        self.optimizer = optim.Adam(self.q_network.parameters(), lr=learning_rate)
        
        # Initialize your replay buffer
        self.replay_buffer = []

        # Initialize the environment
        self.env = SimpleEnvironment(num_states=10)

    def select_action(self, epsilon=0.3, num_best_action_samples = 20):
        if np.random.rand() < epsilon:
            # Explore: Choose a random action in the continuous space
           return self.select_random_action()
        else:
            # Exploit: Choose the action with the highest Q-value
            possible_actions = [torch.FloatTensor(np.random.uniform(-1, 1, size=(self.action_dim,))) for _ in range(num_best_action_samples)]  # Sample some random actions for exploration
            q_values = [self.q_network(action).item() for action in possible_actions]
            chosen_action = possible_actions[np.argmax(q_values)]

        return chosen_action


    def select_random_action(self):
        # Generate a random action in the continuous space
        random_action = torch.FloatTensor(np.random.uniform(-1, 1, size=(self.action_dim,)))

        # Calculate Q-value for the random action
        q_value = self.q_network(random_action)

        # UCB exploration-exploitation strategy
        exploration_bonus_ucb = self.exploration_coefficient * np.sqrt(np.log(len(self.replay_buffer) + 1))

        # Thompson Sampling exploration
        exploration_bonus_thompson = scipy.stats.norm.rvs(loc=q_value.item(), scale=1.0)

        # Combine UCB and Thompson Sampling
        combined_bonus = exploration_bonus_ucb + exploration_bonus_thompson

        # Perturb the random action with the combined bonus
        chosen_action = random_action + combined_bonus

        return chosen_action


    def train(self, batch_size=32):
        if len(self.replay_buffer) < batch_size:
            return

        # Sample a random batch from the replay buffer
        batch = np.random.choice(self.replay_buffer, batch_size, replace=False)
        actions, rewards, next_actions, dones = zip(*batch)

        # Convert to PyTorch tensors
        actions = torch.FloatTensor(actions)
        rewards = torch.FloatTensor(rewards)
        next_actions = torch.FloatTensor(next_actions)
        dones = torch.FloatTensor(dones)

        # Compute target Q values
        target_q_values = rewards + self.gamma * torch.max(self.q_network(next_actions)).item() * (1 - dones)

        # Compute current Q values
        current_q_values = self.q_network(actions)

        # Compute the loss
        loss = nn.MSELoss()(current_q_values, target_q_values.unsqueeze(1))

        # Update the Q-network
        self.optimizer.zero_grad()
        loss.backward()
        self.optimizer.step()

    def add_to_replay_buffer(self, action, reward, next_action, done):
        self.replay_buffer.append((action, reward, next_action, done))

    def train_episodes(self, num_episodes):
        for episode in range(num_episodes):
            total_reward = 0

            while True:
                action = self.select_action()
                # Replace the line below with your environment interaction to get the next_action, reward, done
                next_action, reward, done = self.env.step(action)

                self.add_to_replay_buffer(action, reward, next_action, done)
                self.train()

                total_reward += reward

                if done:
                    break

            print(f"Episode: {episode + 1}, Total Reward: {total_reward}")

# Example usage:
action_dim = 5
agent = DeepQAgent(action_dim)
agent.train_episodes(num_episodes=100)
