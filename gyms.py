import discord
from .utils import checks
from discord.ext import commands
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, query, Q, DocType, Text, Keyword, GeoPoint
from elasticsearch_dsl.connections import connections
import elasticsearch
import datetime
import re
import json
import asyncio
import csv
import time
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import (
    create_engine, Column, Integer,
    String, DateTime, Float, ForeignKey, Boolean, UniqueConstraint)
from sqlalchemy.orm import sessionmaker, relationship

Base = declarative_base()

SETTINGS = [
    "mirror",
    "delete_on_done",
    "location",
    "scale",
    "emoji_going",
    "emoji_plus1",
    "emoji_minus1",
    "emoji_command",
    "edit_time",
    "emoji_add_time",
    "emoji_remove_time",
    "emoji_done",
    "emoji_mystic",
    "emoji_valor",
    "emoji_instinct",
    "role_mystic",
    "role_valor",
    "role_instinct",
    "enable_subscriptions",
]

RE_DISCORD_MENTION = re.compile("\<@(?:\!|)(\d+)\>")
RE_EMOJI = re.compile("\<\:(.+):(\d+)>")

TIME_STRING = "Invalid time specified, please use HH:MM, HHMM, HH.MM, Xm or \"YYYY-MM-DD HH:MM\""

class Gym(Base):
    __tablename__ = 'gym'
    id = Column(Integer, primary_key=True)
    title = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    

class Pokemon(Base):
    __tablename__ = 'pokemon'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    raid_level = Column(Integer, nullable=True)


class Raid(Base):
    __tablename__ = 'raid'
    id = Column(Integer, primary_key=True)
    pokemon_id = Column(Integer, ForeignKey("pokemon.id"), nullable=True)
    pokemon = relationship(Pokemon, foreign_keys=[pokemon_id])
    gym_id = Column(Integer, ForeignKey("gym.id"))
    gym = relationship(Gym, foreign_keys=[gym_id])
    start_time = Column(DateTime)
    level = Column(Integer, nullable=True)
    done = Column(Boolean, default=False)

class Embed(Base):
    __tablename__ = 'embed'
    id = Column(Integer, primary_key=True)
    channel_id = Column(Integer)
    message_id = Column(Integer)
    raid_id = Column(Integer, ForeignKey("raid.id"))
    raid = relationship(Raid, foreign_keys=[raid_id])


class Going(Base):
    __tablename__ = 'going'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    raid_id = Column(Integer, ForeignKey("raid.id"))
    raid = relationship(Raid, foreign_keys=[raid_id])
    extra = Column(Integer, default=0)

class ServerConfig(Base):
    __tablename__ = 'serverconfig'
    id = Column(Integer, primary_key=True)
    server_id = Column(Integer)
    key = Column(String)
    value = Column(String)
    __table_args__ = (UniqueConstraint('server_id', 'key', name='_server_id_key_uc'),)

class ChannelConfig(Base):
    __tablename__ = 'channelconfig'
    id = Column(Integer, primary_key=True)
    server_id = Column(Integer)
    channel_id = Column(Integer)
    key = Column(String)
    value = Column(String)
    __table_args__ = (UniqueConstraint('server_id', 'channel_id', 'key', name='_server_id_key_uc'),)

connections.create_connection(hosts=['localhost'])

class GymDoc(DocType):
    title = Text(analyzer='snowball', fields={'raw': Keyword()})
    location = GeoPoint()

    class Meta:
        index = 'marker'

GymDoc.init()

class PokemonDoc(DocType):
    name = Text(analyzer='snowball', fields={'raw': Keyword()})

    class Meta:
        index = 'pokemon'

PokemonDoc.init()
                
def format_list(items):
    if len(items) > 1:
        message = ", ".join([item for item in items[:-1]])+" and {0}".format(items[-1])
    else:
        message = "{0}".format(items[0])
    return message


class Gyms:
    """Information about gyms, and raid enrollment."""

    def __init__(self, bot):
        self.bot = bot
        self.client = Elasticsearch()
        engine = create_engine('sqlite:///gyms.db')
        Base.metadata.create_all(engine)
        self.session = sessionmaker(bind=engine)()

        self.member_cache = {}
        self.channel_cache = {}

        self.emoji = {}

        for emoji in self.bot.get_all_emojis():
            self.emoji[emoji.name] = str(emoji)

    def get_server_config(self, server_id, key, default=None):
        try:
            config = self.session.query(ServerConfig).filter_by(server_id=server_id, key=key).one()
            return config.value
        except NoResultFound:
            return default

    def set_server_config(self, server_id, key, value):
        try:
            config = self.session.query(ServerConfig).filter_by(server_id=server_id, key=key).one()
            config.value = value
        except NoResultFound:
            config = ServerConfig(server_id=server_id, key=key, value=value)
        self.session.add(config)
        self.session.commit()
        
    def get_channel_config(self, server_id, channel_id, key, default=None):
        try:
            config = self.session.query(ChannelConfig).filter_by(server_id=server_id, channel_id=channel_id, key=key).one()
            return config.value
        except NoResultFound:
            return default

    def set_channel_config(self, server_id, channel_id, key, value):
        try:
            config = self.session.query(ChannelConfig).filter_by(server_id=server_id, channel_id=channel_id, key=key).one()
            config.value = value
        except NoResultFound:
            config = ChannelConfig(server_id=server_id, channel_id=channel_id, key=key, value=value)
        self.session.add(config)
        self.session.commit()

    def get_config(self, channel, key, default=None):
        config = self.get_channel_config(channel.server.id, channel.id, key)
        if config is None:
            config = self.get_server_config(channel.server.id, key)
        if config is None:
            return default
        return config

    def get_emoji(self, emoji):
        if self.emoji == {}:
            for e in self.bot.get_all_emojis():
                self.emoji[str(e)] = e
        return self.emoji.get(emoji, emoji)

    def get_emoji_by_name(self, emoji):
        for e in self.bot.get_all_emojis():
            if e.name == emoji:
                return e
        return emoji

    async def get_display_name(self, channel, member, extra=0):
        team_emoji = ''
        role_mystic = self.get_config(channel, "role_mystic")
        role_valor = self.get_config(channel, "role_valor")
        role_instinct = self.get_config(channel, "role_instinct")
        for role in member.roles:
            if role.name == role_mystic:
                team_emoji = self.get_config(channel, "emoji_mystic")
            elif role.name == role_valor:
                team_emoji = self.get_config(channel, "emoji_valor")
            elif role.name == role_instinct:
                team_emoji = self.get_config(channel, "emoji_instinct")
        name = member.nick if member.nick else str(member.name)
        if team_emoji:
            name = "{} {}".format(team_emoji, name)
        if extra > 0:
            name = "{} (+{})".format(name, extra)
        return name

    async def find_gym(self, gym, channel=None):
        location = self.get_config(channel, "location", [])
        if location != []:
            location = location.replace(" ", "")
            location = location.split(",")
        if channel is not None and len(location) == 2:
            scale = self.get_config(channel, "scale", "2km")
            s = Search(using=self.client, index="marker")
            q = query.Q(
                "function_score",
                query=query.Q("match", title={'query': gym, 'fuzziness': 2}),
                functions=[
                    query.SF("gauss", location={"origin": {"lat": location[0], "lon": location[1]}, "scale": scale})
                ]
            )
            s = s.query(q)
        else:
            s = Search(using=self.client, index="marker").query("match", title={'query': gym, 'fuzziness': 2})
        response = s.execute()
        if response.hits.total == 0:
            return None
        return response[0]

    async def find_pokemon(self, gym):
        s = Search(using=self.client, index="pokemon").query("match", name={'query': gym, 'fuzziness': 2})
        response = s.execute()
        if response.hits.total == 0:
            return None
        return response[0]

    async def get_member(self, server, user_id):
        if user_id in self.member_cache:
            return self.member_cache[user_id]
        user = server.get_member(str(user_id))
        self.member_cache[user_id] = user
        return user

    async def get_channel(self, channel_id):
        if channel_id in self.channel_cache:
            return self.channel_cache[user_id]
        return self.bot.get_channel(str(channel_id))

    async def prepare_raid_embed(self, channel, raid, include_role=False):
        server = channel.server
        if raid.start_time - datetime.datetime.now() > datetime.timedelta(days=1):
            start_time = raid.start_time.strftime("%Y-%m-%d %H:%M")
        else:
            start_time = raid.start_time.strftime("%H:%M")
        title = "{} (#{})".format(raid.gym.title, raid.id)
        going = self.session.query(Going).filter_by(raid=raid)

        async def get_member_with_extra(server, user_id, extra):
            return await self.get_member(server, user_id), extra

        users = []
        num_extra = 0
        if going.count():
            tasks = []
            for g in going:
                num_extra += g.extra
                tasks.append(get_member_with_extra(server, g.user_id, g.extra))
            done, not_done = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)

            tasks = []
            for task in done:
                result, extra = task.result()
                tasks.append(self.get_display_name(channel, result, extra))
            done, not_done = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)

            for task in done:
                result = task.result()
                users.append(result)
        if raid.pokemon is None:
            description = "**Level**: {}\n".format(raid.level)
            image = "https://www.trainerdex.co.uk/egg/{}.png".format(raid.level)
        else:
            image = "https://www.trainerdex.co.uk/pokemon/{}.png".format(raid.pokemon.id)
            if raid.pokemon.raid_level:
                description = "**Pokemon**: {} (Level {})\n".format(raid.pokemon.name, raid.pokemon.raid_level)
            else:
                description = "**Pokemon**: {}\n".format(raid.pokemon.name, raid.pokemon.raid_level)
        description += "**Start Time**: {}\n".format(start_time)
        description += "**Going ({})**\n".format(going.count()+num_extra)

        description += " | ".join(users)
        description += "\nPress the {} below if you are going\n[Click here](https://github.com/Azelphur/EkPoGo-Discord-Bot/wiki/Using-the-bot) more info about this bot".format(self.get_emoji(self.get_config(channel, "emoji_going", u"\U0001F44D")))
        if raid.done:
            embed=discord.Embed(title=title, url="https://www.google.com/maps/dir/Current+Location/{},{}".format(raid.gym.latitude, raid.gym.longitude), description=description, color=0x00FF00)
        else:
            embed=discord.Embed(title=title, url="https://www.google.com/maps/dir/Current+Location/{},{}".format(raid.gym.latitude, raid.gym.longitude), description=description)

        embed.set_thumbnail(url=image)
        embed.set_footer(text="Raid ID {}. Ignore emoji counts, they are inaccurate.".format(raid.id))
        
        content = None
        if include_role and self.get_config(channel, "enable_subscriptions", True):
            role = None
            for _role in channel.server.roles:
                if _role.name == raid.gym.title:
                    role = _role
            if role:
                content = role.mention
        return embed, content

    def prepare_gym_embed(self, gym):
        description = "[Get Directions](https://www.google.com/maps/dir/Current+Location/{},{})".format(gym.location['lat'], gym.location['lon'])
        embed=discord.Embed(title=gym.title, url="https://www.google.com/maps/dir/Current+Location/{},{}".format(gym.location['lat'], gym.location['lon']))
        embed.set_image(url='https://maps.googleapis.com/maps/api/staticmap?center={0},{1}&zoom=15&size=250x125&maptype=roadmap&markers=color:{3}%7C{0},{1}&key={2}'.format(gym.location['lat'], gym.location['lon'], 'AIzaSyCEadifeA8X02v2OKv-orZWm8nQf1Q2EZ4', 'red'))
        embed.set_footer(text="Gym ID {}.".format(gym.meta["id"]))
        return embed

    @commands.command(pass_context=True)
    async def gym(self, ctx, *, gym_title: str):
        """
            Lookup a gym, responds with an image, title and a google maps link.
        """
        gym = await self.find_gym(gym_title, ctx.message.channel)
        if not gym:
            await self.bot.say("Gym not found.")
            return
        await self.bot.say(embed=self.prepare_gym_embed(gym))

    def add_gym(self, title, latitude, longitude):
        gym = Gym(
            title=title,
            latitude=latitude,
            longitude=longitude,
        )
        self.session.add(gym)
        self.session.commit()

        gymdoc = GymDoc(
            meta={'id': gym.id},
            title=title,
            location={"lat": latitude, "lon": longitude}
        )
        gymdoc.save()
        return gym, gymdoc

    @commands.command(pass_context=True)
    @checks.is_owner()
    async def loaddata(self, ctx, *, csv_path="gymdata.json"):
        """
            Load pokemon and gyms from json file
        """
        try:
            with open(csv_path, "r") as f:
                try:
                    data = json.loads(f.read())
                except json.decoder.JSONDecodeError as e:
                    await self.bot.say(e)
                    return
                message = await self.bot.say("Importing data, this will take a second... (0 / {})".format(len(data)))
                last_time = time.time()
                count_gyms = 0
                count_pokemon = 0
                for i, entry in enumerate(data):
                    now = time.time()
                    if now - last_time > 5 or i == len(data)-1:
                        await self.bot.edit_message(message, "Importing data, this will take a second... ({} / {})".format(i+1, len(data)))
                        last_time = now
                    if entry["type"] == "gym":
                        count_gyms += 1
                        try:
                            gym = self.session.query(Gym).filter_by(
                                title=entry["data"]["title"],
                                latitude=entry["data"]["latitude"],
                                longitude=entry["data"]["longitude"]
                            ).one()
                        except NoResultFound:
                            self.add_gym(
                                entry["data"]["title"],
                                entry["data"]["latitude"],
                                entry["data"]["longitude"]
                            )
                        except Exception as e:
                            print("Error on gym", entry)
                            raise e
                    elif entry["type"] == "pokemon":
                        count_pokemon += 1
                        try:
                            p = self.session.query(Pokemon).filter_by(name=entry["data"]["name"]).one()
                            p.id = entry["data"]["id"]
                            p.raid_level = entry["data"].get("raid_level", None)
                            self.session.add(p)
                        except NoResultFound:
                            p = Pokemon(name=entry["data"]["name"], id=entry["data"]["id"], raid_level=entry["data"].get("raid_level", None))
                            self.session.add(p)
                            PokemonDoc(meta={'id': entry["data"]["id"]}, name=entry["data"]["name"]).save()
                self.session.commit()
                await self.bot.say("Imported {} gyms and {} pokemon".format(count_gyms, count_pokemon))
        except FileNotFoundError:
            await self.bot.say("File not found")

    @commands.command(pass_context=True)
    @checks.is_owner()
    async def gymrm(self, ctx, *, gym_id: int):
        """
            Delete a gym from the database.
        """
        try:
            gym = GymDoc.get(id=gym_id)
            gym.delete()
        except elasticsearch.exceptions.NotFoundError:
            pass
        self.session.query(Gym).filter_by(id=gym_id).delete()
        await self.add_reaction(ctx.message, self.get_config(ctx.message.channel, "emoji_command", u"\U0001F44D"))

    @commands.command(pass_context=True)
    @checks.is_owner()
    async def gymadd(self, ctx, title: str, latitude: float, longitude: float):
        """
            Add a gym to the database
        """
        gym, gymdoc = self.add_gym(title, latitude, longitude)
        await self.bot.say(embed=self.prepare_gym_embed(gymdoc))

    @commands.command(pass_context=True)
    @checks.serverowner_or_permissions(administrator=True)
    async def raidserverconfig(self, ctx, key: str = None, value: str = None):
        """
            Set a server config setting, use `!raidserverconfig` on its own to see a list of settings
        """
        if key is None:
            await self.bot.say("Valid settings: {}".format(", ".join(SETTINGS)))
            return
        if key not in SETTINGS:
            await self.bot.say("Invalid setting")
            return

        if value is None:
            await self.bot.say("{} = {}".format(key, self.get_server_config(ctx.message.channel.server.id, key)))
        else:
            self.set_server_config(ctx.message.channel.server.id, key, value)
            await self.bot.say("Ok, {} = {}".format(key, value))

    @commands.command(pass_context=True)
    @checks.serverowner_or_permissions(administrator=True)
    async def raidchannelconfig(self, ctx, key: str, value: str = None):
        """
            Set a channel config setting, these will be chosen in preference to
            raidserverconfig settings, use `!raidserverconfig` on its own to see a list of settings
        """
        if key is None:
            await self.bot.say("Valid settings: {}".format(", ".join(SETTINGS)))
            return
        if key not in SETTINGS:
            await self.bot.say("Invalid setting")
            return

        if value is None:
            await self.bot.say("{} = {}".format(key, self.get_channel_config(ctx.message.channel.server.id, ctx.message.channel.id, key)))
        else:
            self.set_channel_config(ctx.message.channel.server.id, ctx.message.channel.id, key, value)
            await self.bot.say("Ok, {} = {}".format(key, value))

    @commands.command(pass_context=True)
    async def raidstart(self, ctx, raid_id: int, *, start_time: str):
        """
            Alter the start time on a raid. Start time must be
            HH:MM, HHMM, HH.MM or \"YYYY-MM-DD HH:MM\"
        """
        raid = self.session.query(Raid).get(raid_id)
        if raid is None:
            await self.bot.say("Raid not found")
            return
        start_time = start_time.replace('"', '')
        start_dt = await self.parse_time(start_time)
        if start_dt is None:
            await self.bot.say(TIME_STRING)
            return
        raid.start_time = start_dt
        self.session.add(raid)
        self.session.commit()
        await self.add_reaction(ctx.message, self.get_config(ctx.message.channel, "emoji_command", u"\U0001F44D"))
        await self.update_embeds(ctx.message.channel.server, raid)

    @commands.command(pass_context=True)
    async def raidpokemon(self, ctx, raid_id: int, *, pokemon_name: str):
        """
            Set the pokemon that a raid is on.
        """
        raid = self.session.query(Raid).get(raid_id)
        if raid is None:
            await self.bot.say("Raid not found")
            return

        pokemon = await self.find_pokemon(pokemon_name)
        if not pokemon:
            await self.bot.say("Pokemon not found.")
            return
        pokemon = self.session.query(Pokemon).get(pokemon.meta['id'])
        raid.pokemon = pokemon

        self.session.add(raid)
        self.session.commit()
        await self.add_reaction(ctx.message, self.get_config(ctx.message.channel, "emoji_command", u"\U0001F44D"))
        await self.update_embeds(ctx.message.channel.server, raid)

    @commands.command(pass_context=True)
    async def raidin(self, ctx, raid_id: int):
        """
            Mentions everyone who is marked as going
            to a raid, and tells them to go in.
        """
        raid = self.session.query(Raid).get(raid_id)
        if raid is None:
            await self.bot.say("Raid not found")
            return

        going = self.session.query(Going).filter_by(raid=raid)

        users = []
        for g in going:
            member = await self.get_member(ctx.message.channel.server, g.user_id)
            users.append(member.mention)
        msg = "Go in! {}".format(", ".join(users))
        await self.bot.say(msg)
        

    @commands.command(pass_context=True)
    async def raidgym(self, ctx, raid_id: int, *, gym_title: str):
        """
            Change the gym associated with a raid.
        """
        raid = self.session.query(Raid).get(raid_id)
        if raid is None:
            await self.bot.say("Raid not found")
            return

        gym = await self.find_gym(gym_title, ctx.message.channel)
        if not gym:
            await self.bot.say("Gym not found.")
            return

        gym = self.session.query(Gym).get(gym.meta['id'])
        raid.gym = gym
        self.session.add(raid)
        self.session.commit()
        await self.add_reaction(ctx.message, self.get_config(ctx.message.channel, "emoji_command", u"\U0001F44D"))
        await self.update_embeds(ctx.message.channel.server, raid)

    @commands.command(pass_context=True)
    async def raidstats(self, ctx, since: str, *, gym_title: str):
        """
            Statistics about raids on a certain gym
            Useful for EX raid calculations.
            Since must be in YYYY-MM-DD format.
        """

        try:
            start_dt = datetime.datetime.strptime(since, "%Y-%m-%d")
        except ValueError:
            await self.bot.say("Invalid since given, must be in YYYY-MM-DD format.")
            return

        gym = await self.find_gym(gym_title, ctx.message.channel)
        if not gym:
            await self.bot.say("Gym not found.")
            return

        gym = self.session.query(Gym).get(gym.meta['id'])
        raids = self.session.query(Raid).filter(Raid.gym==gym, Raid.start_time >= start_dt)
        num_raids = raids.count()
        total_hits = 0
        extras = 0
        individuals = set()
        for raid in raids:
            going = self.session.query(Going).filter_by(raid=raid)
            for g in going:
                individuals.add(g.user_id)
                total_hits += g.extra
                extras += g.extra
            total_hits += going.count()
        msg = "Since {}, there have been {} raids, {} visits and {} - {} unique visits on {}".format(start_dt, num_raids, total_hits, len(individuals), len(individuals)+extras, gym.title)
        await self.bot.say(msg)

    @commands.command(pass_context=True)
    async def raidgoing(self, ctx, *args):
        """
            Add users as going to a raid.
        """
        if len(args) < 2 or not args[0].isnumeric():
            await self.bot.say("```!raidgoing <raid_id> <member> [extra=0] [<member> [extra=0]...]\n\nAdd users as going to a raid```")
            return

        raid = self.session.query(Raid).get(args[0])
        if raid is None:
            await self.bot.say("Raid not found")
            return

        members = []
        member = None
        skip_next = False
        for i in range(1, len(args)):
            if skip_next:
                skip_next = False
                continue
            match = RE_DISCORD_MENTION.match(args[i])
            if match:
                member = await self.get_member(ctx.message.channel.server, match.group(1))
                extra = 0
                if i+1 < len(args) and args[i+1].isnumeric():
                    extra = int(args[i+1])
                    skip_next = True
                members.append((member, extra))
            else:
                await self.bot.say("```!raidgoing <raid_id> <member> [extra=0] [<member> [extra=0]...]\n\nAdd users as going to a raid```")
                return

        for member, extra in members:
            try:
                going = self.session.query(Going).filter_by(raid=raid, user_id=member.id).one()
            except NoResultFound:
                going = Going(raid=raid, user_id=member.id)
            going.extra = extra
            self.session.add(going)

        self.session.commit()
        await self.add_reaction(ctx.message, self.get_config(ctx.message.channel, "emoji_command", u"\U0001F44D"))
        await self.update_embeds(ctx.message.channel.server, raid)

    async def parse_time(self, start_time):
        start_dt = None
        for t_format in ["%H:%M", "%H%M", "%H.%M"]:
            try:
                now = datetime.datetime.now()
                start_dt = datetime.datetime.strptime(start_time, t_format)
                start_dt = start_dt.replace(
                    year=now.year,
                    month=now.month,
                    day=now.day,
                )
                if start_dt < now:
                    start_dt = start_dt + datetime.timedelta(days=1)
            except ValueError:
                pass

        try:
            start_dt = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M")
        except ValueError:
            pass

        if start_dt is None and start_time[-1].lower() == "m" and start_time[:-1].isnumeric():
            start_dt = datetime.datetime.now() + datetime.timedelta(minutes=int(start_time[:-1]))

        return start_dt


    async def start_raid(self, ctx, start_time, egg_level, pokemon_name, gym_title):
        gym = await self.find_gym(gym_title, ctx.message.channel)
        if not gym:
            await self.bot.say("Gym not found.")
            return

        if pokemon_name is None:
            pokemon = None
        else:
            pokemon = await self.find_pokemon(pokemon_name)
            if not pokemon:
                await self.bot.say("Pokemon not found.")
                return

        start_dt = await self.parse_time(start_time)

        if not start_dt:
            await self.bot.say(TIME_STRING)
            return
        
        if pokemon:
            pokemon = self.session.query(Pokemon).get(pokemon.meta['id'])

        gym = self.session.query(Gym).get(gym.meta['id'])
        raid = Raid(
            pokemon=pokemon,
            gym=gym,
            start_time=start_dt,
            level=egg_level
        )
        self.session.add(raid)
        self.session.commit() # Required as we need raids ID in the embed

        embed, content = await self.prepare_raid_embed(ctx.message.channel, raid, include_role=True)
        tasks = []
        tasks.append(self.bot.say(embed=embed, content=content))
        this_channel = ctx.message.channel.id

        configs = self.session.query(ChannelConfig).filter_by(server_id=ctx.message.channel.server.id, key="mirror", value="yes")
        for config in configs:
            if config.channel_id == this_channel:
                continue
            channel = await self.get_channel(config.channel_id)
            embed, content = await self.prepare_raid_embed(channel, raid)
            tasks.append(self.bot.send_message(
                channel,
                embed=embed,
                content=content))

        done, not_done = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
        tasks = []
        for task in done:
            msg = task.result()
            embed = Embed(channel_id=msg.channel.id, message_id=msg.id, raid=raid)
            self.session.add(embed)
            tasks.append(self.add_reactions(msg))

        done, not_done = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
        for task in done:
            task.result() # This will cause errors to be raised correctly.
        self.session.commit()

    @commands.command(pass_context=True)
    async def raidmirror(self, ctx, raid_id: int):
        """
            Mirror a raid embed to this channel
        """
        raid = self.session.query(Raid).get(raid_id)
        if raid is None:
            await self.bot.say("Raid not found")
            return

        embed, content = await self.prepare_raid_embed(ctx.message.channel, raid)
        msg = await self.bot.say(embed=embed, content=content)
        embed = Embed(channel_id=msg.channel.id, message_id=msg.id, raid=raid)
        self.session.add(embed)
        self.session.commit()
        await self.add_reactions(msg)

    @commands.command(pass_context=True)
    async def raidhide(self, ctx, raid_id: int, channel: discord.Channel = None):
        """
            Hide a raid embed from a channel
        """
        raid = self.session.query(Raid).get(raid_id)
        if raid is None:
            await self.bot.say("Raid not found")
            return

        if channel is None:
            channel = ctx.message.channel

        embeds = self.session.query(Embed).filter_by(raid=raid, channel_id=channel.id)
        tasks = []
        for embed in embeds:
            self.session.query(Embed).filter_by(id=embed.id).delete()
            tasks.append(self.delete_message(embed))
        if tasks:
            self.session.commit()
            done, not_done = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
            for task in done:
                task.result() # This will cause errors to be raised correctly.

    @commands.command(pass_context=True)
    async def egg(self, ctx, start_time: str, egg_level: int, * , gym_title: str):
        """
            Create a raid on an egg.
        """
        await self.start_raid(ctx, start_time, egg_level, None, gym_title)

    @commands.command(pass_context=True)
    async def raid(self, ctx, start_time: str, pokemon_name: str, *, gym_title: str):
        """
            Create a raid on a pokemon.
        """
        await self.start_raid(ctx, start_time, None, pokemon_name, gym_title)

    @commands.command(pass_context=True)
    async def raidsubscribe(self, ctx, *, gym_title: str):
        """
            Subscribe to notifications on a gym
        """
        if not self.get_config(ctx.message.channel, "enable_subscriptions", True):
            await self.bot.say("This server has raid subscriptions disabled")
            return
        if not ctx.message.channel.server.me.server_permissions.manage_roles:
            await self.bot.say("I do not have permission to manage roles on this server")
            return
        gym = await self.find_gym(gym_title, ctx.message.channel)
        if not gym:
            await self.bot.say("Gym not found.")
            return
        role = None
        for _role in ctx.message.channel.server.roles:
            if _role.name == gym.title:
                role = _role
        if role is None:
            role = await self.bot.create_role(ctx.message.channel.server, name=gym.title, mentionable=True)
        await self.bot.add_roles(ctx.message.author, role)
        await self.bot.say("I've subscribed you to notifications for {}".format(gym.title))

    @commands.command(pass_context=True)
    async def raidunsubscribe(self, ctx, gym_title: str):
        """
            Unsubscribe to notifications on a gym
        """
        if not self.get_config(ctx.message.channel, "enable_subscriptions", True):
            await self.bot.say("This server has raid subscriptions disabled")
            return
        if not ctx.message.channel.server.me.server_permissions.manage_roles:
            await self.bot.say("I do not have permission to manage roles on this server")
            return
        gym = await self.find_gym(gym_title, ctx.message.channel)
        if not gym:
            await self.bot.say("Gym not found.")
            return
        role = None
        for _role in ctx.message.channel.server.roles:
            if _role.name == gym.title:
                role = _role
        if role is None:
            await self.bot.say("You are already unsubscribed from this gym")
            return
        await self.bot.remove_roles(ctx.message.author, role)
        await self.bot.say("I've unsubscribed you to notifications for {}".format(gym.title))

    @commands.command(pass_context=True)
    @checks.serverowner_or_permissions(administrator=True)
    async def redo_reactions(self, ctx):
        from_time = datetime.datetime.now() - datetime.timedelta(days=14)
        raids = self.session.query(Raid).filter(Raid.start_time >= from_time)
        count = raids.count()
        progress_msg = await self.bot.say("Processing... 0 / {}".format(count))
        last_time = time.time()
        i = 0
        for raid in raids:
            embeds = self.session.query(Embed).filter_by(raid=raid)
            for embed in embeds:
                if time.time() - last_time > 10:
                    await self.bot.edit_message(progress_msg, "Processing... {} / {}".format(i, count))
                    last_time = time.time()
                channel = await self.get_channel(embed.channel_id)
                if channel is None:
                    self.session.query(Embed).filter_by(id=embed.id).delete()
                    continue
                message = await self.get_message(channel, embed.message_id)
                if message is None:
                    self.session.query(Embed).filter_by(id=embed.id).delete()
                    continue
                await self.bot.clear_reactions(message)
                await self.add_reactions(message)
            i = i + 1
        self.session.commit()
        await self.bot.edit_message(message, "Processing... {} / {}".format(count, count))
        await self.bot.say("Done")

    async def add_reaction(self, msg, emoji):
        match = RE_EMOJI.match(emoji)
        if match:
            emoji = self.get_emoji(emoji)
        await self.bot.add_reaction(msg, emoji)

    async def add_reactions(self, msg):
        await self.add_reaction(msg, self.get_config(msg.channel, "emoji_going", u"\U0001F44D"))
        await self.add_reaction(msg, self.get_config(msg.channel, "emoji_plus1", u"\U00002B06"))
        await self.add_reaction(msg, self.get_config(msg.channel, "emoji_minus1", u"\U00002B07"))
        await self.add_reaction(msg, self.get_config(msg.channel, "emoji_add_time", u"\U000023E9"))
        await self.add_reaction(msg, self.get_config(msg.channel, "emoji_remove_time", u"\U000023EA"))
        await self.add_reaction(msg, self.get_config(msg.channel, "emoji_done", u"\U00002705"))

    async def get_message(self, channel, message_id):
        # Load message from cache, otherwise fetch it.
        message = discord.utils.get(self.bot.messages, id=id)
        return message if message else await self.bot.get_message(channel, message_id)

    async def update_embed(self, embed, raid):
        channel = await self.get_channel(embed.channel_id)
        message = await self.get_message(channel, embed.message_id)
        discord_embed, content = await self.prepare_raid_embed(channel, raid)
        await self.bot.edit_message(message, embed=discord_embed)

    async def delete_message(self, embed):
        channel = await self.get_channel(embed.channel_id)
        message = await self.get_message(channel, embed.message_id)
        await self.bot.delete_message(message)

    async def update_embeds(self, server, raid):
        embeds = self.session.query(Embed).filter_by(raid=raid)
        tasks = []
        for embed in embeds:
            tasks.append(self.update_embed(embed, raid))
        if tasks:
            await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)

    async def on_raw_reaction(self, emoji, message_id, channel_id, user_id):

        channel = await self.get_channel(channel_id)
        message = await self.get_message(channel, message_id)
        emoji = self.get_emoji_by_name(emoji)

        if message.author == self.bot.user and user_id != self.bot.user.id:
            try:
                embed = self.session.query(Embed).filter_by(channel_id=channel_id, message_id=message_id).one()
            except NoResultFound:
                return
            
            if embed is None:
                return

            emoji_going = self.get_emoji(self.get_config(channel, "emoji_going", u"\U0001F44D"))
            emoji_plus1 = self.get_emoji(self.get_config(channel, "emoji_plus1", u"\U00002B06"))
            emoji_minus1 = self.get_emoji(self.get_config(channel, "emoji_minus1", u"\U00002B07"))
            emoji_add_time = self.get_emoji(self.get_config(channel, "emoji_add_time", u"\U000023E9"))
            emoji_remove_time = self.get_emoji(self.get_config(channel, "emoji_remove_time", u"\U000023EA"))
            emoji_done = self.get_emoji(self.get_config(channel, "emoji_done", u"\U00002705"))

            if emoji == emoji_going:
                try:
                    going = self.session.query(Going).filter_by(raid=embed.raid, user_id=user_id).one()
                    self.session.query(Going).filter_by(raid=embed.raid, user_id=user_id).delete()
                except NoResultFound:
                    going = Going(raid=embed.raid, user_id=user_id, extra=0)
                    self.session.add(going)
                self.session.commit()

                await self.update_embeds(channel.server, embed.raid)
            elif emoji in [emoji_plus1, emoji_minus1]:
                try:
                    going = self.session.query(Going).filter_by(raid=embed.raid, user_id=user_id).one()
                except NoResultFound:
                    return
                if emoji == emoji_plus1:
                    going.extra = going.extra + 1
                elif going.extra == 0:
                    return
                else:
                    going.extra = going.extra - 1
                self.session.add(going)
                self.session.commit()
                await self.update_embeds(channel.server, embed.raid)
            elif emoji in [emoji_add_time, emoji_remove_time]:
                raid = embed.raid
                if emoji == emoji_add_time:
                    raid.start_time += datetime.timedelta(minutes=int(self.get_config(channel, "edit_time", 5)))
                else:
                    raid.start_time -= datetime.timedelta(minutes=int(self.get_config(channel, "edit_time", 5)))
                self.session.add(raid)
                self.session.commit()
                await self.update_embeds(channel.server, embed.raid)
            elif emoji == emoji_done:
                raid = embed.raid
                raid.done = not raid.done
                self.session.add(raid)
                self.session.commit()
                embeds = self.session.query(Embed).filter_by(raid=raid)
                if raid.done:
                    tasks = []
                    for embed in embeds:
                        embed_channel = await self.get_channel(embed.channel_id)
                        if self.get_config(embed_channel, "delete_on_done", "no") == "no":
                            continue
                        self.session.query(Embed).filter_by(id=embed.id).delete()
                        tasks.append(self.delete_message(embed))
                    if tasks:
                        self.session.commit()
                        done, not_done = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
                        for task in done:
                            task.result() # This will cause errors to be raised correctly.
                    await self.update_embeds(channel.server, raid)
                else:
                    await self.update_embeds(channel.server, raid)
                    tasks = []
                    configs = self.session.query(ChannelConfig).filter_by(server_id=channel.server.id, key="delete_on_done")
                    for config in configs:
                        ch = config.channel_id
                        ch_obj = await self.get_channel(ch)
                        embed, content = await self.prepare_raid_embed(ch_obj, raid)
                        tasks.append(self.bot.send_message(
                            ch_obj,
                            embed=embed,
                            content=content))

                    if tasks:
                        done, not_done = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
                        tasks = []
                        for task in done:
                            msg = task.result()
                            embed = Embed(channel_id=msg.channel.id, message_id=msg.id, raid=raid)
                            self.session.add(embed)
                            tasks.append(self.add_reactions(msg))
                        
                        done, not_done = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
                        for task in done:
                            task.result() # This will cause errors to be raised correctly.
                    self.session.commit()


    async def on_raw_message_delete(self, channel_id, message_id):
        try:
            deleted_embed = self.session.query(Embed).filter_by(channel_id=channel_id, message_id=message_id).one()
        except NoResultFound:
            return


        embeds = self.session.query(Embed).filter_by(raid=deleted_embed.raid)
        for embed in embeds:
            if embed.channel_id == int(channel_id) and embed.message_id == int(message_id):
                continue
            try:
                channel = await self.get_channel(embed.channel_id)
                message = await self.get_message(channel, embed.message_id)
                await self.bot.delete_message(message)
            except discord.errors.NotFound:
                print("Message not found!", embed.channel_id, embed.message_id)

        
        self.session.query(Going).filter_by(raid=deleted_embed.raid).delete()
        self.session.query(Embed).filter_by(raid=deleted_embed.raid).delete()
        self.session.query(Raid).filter_by(id=deleted_embed.raid_id).delete()

    async def on_socket_raw_receive(self, msg):
        if not isinstance(msg, str):
            return
        try:
            response = json.loads(msg)
        except json.decoder.JSONDecodeError:
            return
        if response['t'] in ['MESSAGE_REACTION_ADD', 'MESSAGE_REACTION_REMOVE'] and response['d']['user_id'] != self.bot.user.id:
            await self.on_raw_reaction(
                response['d']['emoji']['name'],
                response['d']['message_id'],
                response['d']['channel_id'],
                response['d']['user_id']
            )
        elif response["t"] == "MESSAGE_DELETE":
            await self.on_raw_message_delete(
                response['d']['channel_id'],
                response['d']['id']
            )

def setup(bot):
    bot.add_cog(Gyms(bot))
